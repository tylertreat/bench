/*
Package bench provides a generic framework for performing latency benchmarks.
*/
package bench

import (
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"
)

const (
	maxRecordableLatencyNS = 300000000000
	sigFigs                = 5
)

// RequesterFactory creates new Requesters.
type RequesterFactory interface {
	// GetRequester returns a new Requester, called for each Benchmark
	// connection.
	GetRequester(number uint64) Requester
}

// Requester synchronously issues requests for a particular system under test.
type Requester interface {
	// Setup prepares the Requester for benchmarking.
	Setup() error

	// Request performs a synchronous request to the system under test.
	Request() error

	// Teardown is called upon benchmark completion.
	Teardown() error
}

// Benchmark performs a system benchmark by attempting to issue requests at a
// specified rate and capturing the latency distribution. The request rate is
// divided across the number of configured connections.
type Benchmark struct {
	connections uint64
	benchmarks  []*connectionBenchmark
}

// NewBenchmark creates a Benchmark which runs a system benchmark using the
// given RequesterFactory. The requestRate argument specifies the number of
// requests per second to issue. This value is divided across the number of
// connections specified, so if requestRate is 50,000 and connections is 10,
// each connection will attempt to issue 5,000 requests per second. A zero
// value disables rate limiting entirely. The duration argument specifies how
// long to run the benchmark.
func NewBenchmark(factory RequesterFactory, requestRate, connections uint64,
	duration time.Duration) *Benchmark {

	if connections == 0 {
		connections = 1
	}

	benchmarks := make([]*connectionBenchmark, connections)
	for i := uint64(0); i < connections; i++ {
		benchmarks[i] = newConnectionBenchmark(
			factory.GetRequester(i), requestRate/connections, duration)
	}

	return &Benchmark{connections: connections, benchmarks: benchmarks}
}

// Run the benchmark and return a summary of the results. An error is returned
// if something went wrong along the way.
func (b *Benchmark) Run() (*Summary, error) {
	var (
		start   = make(chan struct{})
		results = make(chan *result, b.connections)
		wg      sync.WaitGroup
	)

	// Prepare connection benchmarks
	for _, benchmark := range b.benchmarks {
		if err := benchmark.setup(); err != nil {
			return nil, err
		}
		wg.Add(1)
		go func(b *connectionBenchmark) {
			<-start
			results <- b.run()
			wg.Done()
		}(benchmark)
	}

	// Start benchmark
	close(start)

	// Wait for completion
	wg.Wait()

	// Teardown
	for _, benchmark := range b.benchmarks {
		if err := benchmark.teardown(); err != nil {
			return nil, err
		}
	}

	// Merge results
	result := <-results
	if result.err != nil {
		return nil, result.err
	}
	summary := result.summary
	for i := uint64(1); i < b.connections; i++ {
		result = <-results
		if result.err != nil {
			return nil, result.err
		}
		summary.merge(result.summary)
	}
	summary.Connections = b.connections

	return summary, nil
}

// result of a single connectionBenchmark run.
type result struct {
	err     error
	summary *Summary
}

// connectionBenchmark performs a system benchmark by issuing requests at a
// specified rate and capturing the latency distribution.
type connectionBenchmark struct {
	requester            Requester
	requestRate          uint64
	duration             time.Duration
	expectedInterval     time.Duration
	histogram            *hdrhistogram.Histogram
	uncorrectedHistogram *hdrhistogram.Histogram
	totalRequests        uint64
	elapsed              time.Duration
}

// newConnectionBenchmark creates a connectionBenchmark which runs a system
// benchmark using the given Requester. The requestRate argument specifies the
// number of requests per second to issue. A zero value disables rate limiting
// entirely. The duration argument specifies how long to run the benchmark.
func newConnectionBenchmark(requester Requester, requestRate uint64, duration time.Duration) *connectionBenchmark {
	var interval time.Duration
	if requestRate > 0 {
		interval = time.Duration(1000000000 / requestRate)
	}

	return &connectionBenchmark{
		requester:            requester,
		requestRate:          requestRate,
		duration:             duration,
		expectedInterval:     interval,
		histogram:            hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
		uncorrectedHistogram: hdrhistogram.New(1, maxRecordableLatencyNS, sigFigs),
	}
}

// setup prepares the benchmark for running.
func (c *connectionBenchmark) setup() error {
	c.histogram.Reset()
	c.uncorrectedHistogram.Reset()
	c.totalRequests = 0
	return c.requester.Setup()
}

// teardown cleans up any benchmark resources.
func (c *connectionBenchmark) teardown() error {
	return c.requester.Teardown()
}

// run the benchmark and return the result. Result contains an error if
// something went wrong along the way.
func (c *connectionBenchmark) run() *result {
	var err error
	if c.requestRate == 0 {
		c.elapsed, err = c.runFullThrottle()
	} else {
		c.elapsed, err = c.runRateLimited()
	}
	return &result{summary: c.summarize(), err: err}
}

// runRateLimited runs the benchmark by attempting to issue the configured
// number of requests per second.
func (c *connectionBenchmark) runRateLimited() (time.Duration, error) {
	var (
		interval = c.expectedInterval.Nanoseconds()
		stop     = time.After(c.duration)
		start    = time.Now()
	)
	for {
		select {
		case <-stop:
			return time.Since(start), nil
		default:
		}

		before := time.Now()
		if err := c.requester.Request(); err != nil {
			return 0, err
		}
		latency := time.Since(before).Nanoseconds()
		if err := c.histogram.RecordCorrectedValue(latency, interval); err != nil {
			return 0, err
		}
		if err := c.uncorrectedHistogram.RecordValue(latency); err != nil {
			return 0, err
		}
		c.totalRequests++

		for c.expectedInterval > (time.Now().Sub(before)) {
			// Busy spin
		}
	}
}

// runFullThrottle runs the benchmark without a limit on requests per second.
func (c *connectionBenchmark) runFullThrottle() (time.Duration, error) {
	var (
		stop  = time.After(c.duration)
		start = time.Now()
	)
	for {
		select {
		case <-stop:
			return time.Since(start), nil
		default:
		}

		before := time.Now()
		if err := c.requester.Request(); err != nil {
			return 0, err
		}
		if err := c.histogram.RecordValue(time.Since(before).Nanoseconds()); err != nil {
			return 0, err
		}
		c.totalRequests++
	}
}

// summarize returns a Summary of the last benchmark run.
func (c *connectionBenchmark) summarize() *Summary {
	return &Summary{
		TotalRequests:        c.totalRequests,
		TimeElapsed:          c.elapsed,
		Histogram:            hdrhistogram.Import(c.histogram.Export()),
		UncorrectedHistogram: hdrhistogram.Import(c.uncorrectedHistogram.Export()),
		Throughput:           float64(c.totalRequests) / c.elapsed.Seconds(),
		RequestRate:          c.requestRate,
	}
}
