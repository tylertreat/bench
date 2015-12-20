/*
Package bench provides a generic framework for performing latency benchmarks.
*/
package bench

import (
	"fmt"
	"os"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/tsenart/tb"
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
	rateLimit            int64
	interval             time.Duration
	tb                   *tb.Bucket
	numRequests          int64
	histogram            *hdrhistogram.Histogram
	uncorrectedHistogram *hdrhistogram.Histogram
}

// NewBenchmark creates a Benchmark which runs a system benchmark using the
// given Requester. The rateLimit argument specifies the number of requests per
// interval to issue. A negative value disables rate limiting entirely. The
// numRequests argument specifies the total number of requests to issue in the
// benchmark.
func NewBenchmark(requester Requester, rateLimit int64, interval time.Duration,
	numRequests int64) *Benchmark {

	if interval <= 0 {
		interval = time.Second
	}

	return &Benchmark{
		requester:            requester,
		rateLimit:            rateLimit,
		interval:             interval,
		tb:                   tb.NewBucket(rateLimit, interval),
		numRequests:          numRequests,
		histogram:            hdrhistogram.New(0, maxRecordableLatencyNS, sigFigs),
		uncorrectedHistogram: hdrhistogram.New(0, maxRecordableLatencyNS, sigFigs),
	}
}

// Run the benchmark and return an error if something went wrong along the way.
// This can be called multiple times, overwriting the results on each call,
// unless Dispose is called.
func (b *Benchmark) Run() error {
	b.histogram.Reset()
	b.uncorrectedHistogram.Reset()
	b.tb.Put(b.rateLimit)

	if err := b.requester.Setup(); err != nil {
		return err
	}

	var err error
	if b.rateLimit < 0 {
		err = b.runFullThrottle()
	} else {
		err = b.runRateLimited()
	}

	if e := b.requester.Teardown(); e != nil {
		err = e
	}

	return err
}

// Dispose resources used by the Benchmark. Once this is called, this Benchmark
// can no longer be used.
func (b *Benchmark) Dispose() {
	b.tb.Close()
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
		value := b.histogram.ValueAtQuantile(percentile)
		_, err := f.WriteString(fmt.Sprintf("%d    %f        %d            %f\n",
			value, percentile/100, 0, 1/(1-(percentile/100))))
		if err != nil {
			return err
		}
	}

	// Generate uncorrected distribution.
	if b.rateLimit > 0 {
		f, err := os.Create("uncorrected_" + file)
		if err != nil {
			return err
		}
		defer f.Close()

		f.WriteString("Value    Percentile    TotalCount    1/(1-Percentile)\n\n")
		for _, percentile := range percentiles {
			value := b.uncorrectedHistogram.ValueAtQuantile(percentile)
			_, err := f.WriteString(fmt.Sprintf("%d    %f        %d            %f\n",
				value, percentile/100, 0, 1/(1-(percentile/100))))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *Benchmark) runRateLimited() error {
	for i := int64(0); i < b.numRequests; i++ {
		b.tb.Wait(1)
		before := time.Now()
		if err := b.requester.Request(); err != nil {
			return err
		}
		latency := time.Since(before).Nanoseconds()
		if err := b.histogram.RecordCorrectedValue(latency, b.interval.Nanoseconds()); err != nil {
			return err
		}
		if err := b.uncorrectedHistogram.RecordValue(latency); err != nil {
			return err
		}
	}
	return nil
}

func (b *Benchmark) runFullThrottle() error {
	for i := int64(0); i < b.numRequests; i++ {
		before := time.Now()
		if err := b.requester.Request(); err != nil {
			return err
		}
		if err := b.histogram.RecordValue(time.Since(before).Nanoseconds()); err != nil {
			return err
		}
	}
	return nil
}
