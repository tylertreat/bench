/*
Package bench provides a generic framework for performing latency benchmarks.
*/
package bench

import (
	"fmt"
	"os"
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

// Summary contains the results of a Benchmark run.
type Summary struct {
	RequestRate          uint64
	TotalRequests        uint64
	TimeElapsed          time.Duration
	Histogram            hdrhistogram.Histogram
	UncorrectedHistogram hdrhistogram.Histogram
}

func (s *Summary) String() string {
	return fmt.Sprintf("{RequestRate: %d, TotalRequests: %d, TimeElapsed: %s}",
		s.RequestRate, s.TotalRequests, s.TimeElapsed)
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
	elapsed              time.Duration
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

// Run the benchmark and return an error if something went wrong along the way.
// This can be called multiple times, overwriting the results on each call,
// unless Dispose is called.
func (b *Benchmark) Run() error {
	b.histogram.Reset()
	b.uncorrectedHistogram.Reset()
	b.totalRequests = 0

	if err := b.requester.Setup(); err != nil {
		return err
	}

	var err error
	if b.requestRate == 0 {
		b.elapsed, err = b.runFullThrottle()
	} else {
		b.elapsed, err = b.runRateLimited()
	}

	if e := b.requester.Teardown(); e != nil {
		err = e
	}

	return err
}

// Summary returns a Summary of the last Benchmark run.
func (b *Benchmark) Summary() *Summary {
	return &Summary{
		RequestRate:          b.requestRate,
		TotalRequests:        b.totalRequests,
		TimeElapsed:          b.elapsed,
		Histogram:            *b.histogram,
		UncorrectedHistogram: *b.uncorrectedHistogram,
	}
}

// GenerateLatencyDistribution generates a text file containing the specified
// latency distribution in a format plottable by
// http://hdrhistogram.github.io/HdrHistogram/plotFiles.html. If percentiles is
// nil, it defaults to a logarithmic percentile scale. If a rate limit was
// specified for the benchmark, this will also generate an uncorrected
// distribution file which does not account for coordinated omission.
func (b *Benchmark) GenerateLatencyDistribution(percentiles []float64, file string) error {
	if percentiles == nil {
		percentiles = defaultPercentiles
	}
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString("Value    Percentile    TotalCount    1/(1-Percentile)\n\n")
	for _, percentile := range percentiles {
		value := float64(b.histogram.ValueAtQuantile(percentile)) / 1000000
		_, err := f.WriteString(fmt.Sprintf("%f    %f        %d            %f\n",
			value, percentile/100, 0, 1/(1-(percentile/100))))
		if err != nil {
			return err
		}
	}

	// Generate uncorrected distribution.
	if b.requestRate > 0 {
		f, err := os.Create("uncorrected_" + file)
		if err != nil {
			return err
		}
		defer f.Close()

		f.WriteString("Value    Percentile    TotalCount    1/(1-Percentile)\n\n")
		for _, percentile := range percentiles {
			value := float64(b.uncorrectedHistogram.ValueAtQuantile(percentile)) / 1000000
			_, err := f.WriteString(fmt.Sprintf("%f    %f        %d            %f\n",
				value, percentile/100, 0, 1/(1-(percentile/100))))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

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
		b.totalRequests += 1

		for b.expectedInterval > (time.Now().Sub(before)) {
			// Busy spin
		}
	}
}

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
		b.totalRequests += 1
	}
}
