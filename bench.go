/*
Package bench provides a generic framework for performing latency benchmarks.
*/
package bench

import (
	"sync/atomic"
	"time"

	"github.com/codahale/hdrhistogram"
)

const (
	maxRecordableLatencyNS = 300000000000
	sigFigs                = 5
)

// Requester synchronously issues requests for a particular system under test.
type Requester interface {
	// Setup prepares the Requester for benchmarking.
	Setup() error

	// Request performs a synchronous request to the system under test.
	Request() error

	// Teardown is called upon benchmark completion.
	Teardown() error
}

// Benchmark performs a system benchmark by issuing a certain number of
// requests at a specified rate and capturing the latency distribution.
type Benchmark struct {
	requester            Requester
	requestRate          uint64
	duration             time.Duration
	expectedInterval     time.Duration
	histogram            *hdrhistogram.Histogram
	uncorrectedHistogram *hdrhistogram.Histogram
	totalRequests        uint64
	tickRequests         uint64
	elapsed              time.Duration
	requestsPerSecond    []uint64
}

// NewBenchmark creates a Benchmark which runs a system benchmark using the
// given Requester. The requestRate argument specifies the number of requests
// per second to issue. A zero value disables rate limiting entirely. The
// duration argument specifies how long to run the benchmark.
func NewBenchmark(requester Requester, requestRate uint64, duration time.Duration) *Benchmark {

	var interval time.Duration
	if requestRate > 0 {
		interval = time.Duration(1000000000 / requestRate)
	}

	return &Benchmark{
		requester:            requester,
		requestRate:          requestRate,
		duration:             duration,
		expectedInterval:     interval,
		histogram:            hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
		uncorrectedHistogram: hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
	}
}

// Run the benchmark and return a summary of the results. An error is returned
// if something went wrong along the way.
func (b *Benchmark) Run() (*Summary, error) {
	b.histogram.Reset()
	b.uncorrectedHistogram.Reset()
	b.totalRequests = 0

	if err := b.requester.Setup(); err != nil {
		return nil, err
	}

	var (
		err               error
		stop              = make(chan struct{})
		requestsPerSecond = make(chan []uint64, 1)
	)

	go b.startRequestCounter(stop, requestsPerSecond)

	if b.requestRate == 0 {
		b.elapsed, err = b.runFullThrottle()
	} else {
		b.elapsed, err = b.runRateLimited()
	}

	close(stop)
	b.requestsPerSecond = <-requestsPerSecond

	if e := b.requester.Teardown(); e != nil {
		err = e
	}

	return b.summarize(), err
}

// runRateLimited runs the Benchmark by attempting to issue the configured
// number of requests per second.
func (b *Benchmark) runRateLimited() (time.Duration, error) {
	var (
		interval = b.expectedInterval.Nanoseconds()
		stop     = time.After(b.duration)
		start    = time.Now()
	)
	for {
		select {
		case <-stop:
			return time.Since(start), nil
		default:
		}

		before := time.Now()
		if err := b.requester.Request(); err != nil {
			return 0, err
		}
		latency := time.Since(before).Nanoseconds()
		if err := b.histogram.RecordCorrectedValue(latency, interval); err != nil {
			return 0, err
		}
		if err := b.uncorrectedHistogram.RecordValue(latency); err != nil {
			return 0, err
		}
		b.incrementRequestCount()

		for b.expectedInterval > (time.Now().Sub(before)) {
			// Busy spin
		}
	}
}

// runFullThrottle runs the Benchmark without a limit on requests per second.
func (b *Benchmark) runFullThrottle() (time.Duration, error) {
	var (
		stop  = time.After(b.duration)
		start = time.Now()
	)
	for {
		select {
		case <-stop:
			return time.Since(start), nil
		default:
		}

		before := time.Now()
		if err := b.requester.Request(); err != nil {
			return 0, err
		}
		if err := b.histogram.RecordValue(time.Since(before).Nanoseconds()); err != nil {
			return 0, err
		}
		b.incrementRequestCount()
	}
}

// incrementRequestCount increments the running total number of requests and
// the running number of requests for the current tick.
func (b *Benchmark) incrementRequestCount() {
	b.totalRequests += 1
	atomic.AddUint64(&b.tickRequests, 1)
}

// startRequestCounter ticks every second and collects the number of requests
// occurring in each tick. Closing the stop channel causes the tick counts to
// be put on the result channel and stops the ticker. This should be called in
// a goroutine.
func (b *Benchmark) startRequestCounter(stop <-chan struct{}, result chan<- []uint64) {
	counts := []uint64{}
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			counts = append(counts, atomic.LoadUint64(&b.tickRequests))
			atomic.StoreUint64(&b.tickRequests, 0)
		case <-stop:
			result <- counts
			return
		}
	}
}

// summarize returns a Summary of the last Benchmark run.
func (b *Benchmark) summarize() *Summary {
	return &Summary{
		RequestRate:          b.requestRate,
		TotalRequests:        b.totalRequests,
		TimeElapsed:          b.elapsed,
		Histogram:            hdrhistogram.Import(b.histogram.Export()),
		UncorrectedHistogram: hdrhistogram.Import(b.uncorrectedHistogram.Export()),
		RequestsPerSecond:    b.requestsPerSecond,
	}
}
