package bench

import (
	"fmt"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/tylertreat/hdrhistogram-writer"
)

// Summary contains the results of a Benchmark run.
type Summary struct {
	Connections                 uint64
	RequestRate                 uint64
	SuccessTotal                uint64
	ErrorTotal                  uint64
	TimeElapsed                 time.Duration
	SuccessHistogram            *hdrhistogram.Histogram
	UncorrectedSuccessHistogram *hdrhistogram.Histogram
	ErrorHistogram              *hdrhistogram.Histogram
	UncorrectedErrorHistogram   *hdrhistogram.Histogram
	Throughput                  float64
}

// String returns a stringified version of the Summary.
func (s *Summary) String() string {
	return fmt.Sprintf(
		"\n{Connections: %d, RequestRate: %d, RequestTotal: %d, SuccessTotal: %d, ErrorTotal: %d, TimeElapsed: %s, Throughput: %.2f/s}",
		s.Connections, s.RequestRate, (s.SuccessTotal + s.ErrorTotal), s.SuccessTotal, s.ErrorTotal, s.TimeElapsed, s.Throughput)
}

// GenerateLatencyDistribution generates a text file containing the specified
// latency distribution in a format plottable by
// http://hdrhistogram.github.io/HdrHistogram/plotFiles.html. Percentiles is a
// list of percentiles to include, e.g. 10.0, 50.0, 99.0, 99.99, etc. If
// percentiles is nil, it defaults to a logarithmic percentile scale. If a
// request rate was specified for the benchmark, this will also generate an
// uncorrected distribution file which does not account for coordinated
// omission.
func (s *Summary) GenerateLatencyDistribution(percentiles histwriter.Percentiles, file string) error {
	return generateLatencyDistribution(s.SuccessHistogram, s.UncorrectedSuccessHistogram, s.RequestRate, percentiles, file)
}

// GenerateErrorLatencyDistribution generates a text file containing the specified
// latency distribution (of requests that resulted in errors) in a format plottable by
// http://hdrhistogram.github.io/HdrHistogram/plotFiles.html. Percentiles is a
// list of percentiles to include, e.g. 10.0, 50.0, 99.0, 99.99, etc. If
// percentiles is nil, it defaults to a logarithmic percentile scale. If a
// request rate was specified for the benchmark, this will also generate an
// uncorrected distribution file which does not account for coordinated
// omission.
func (s *Summary) GenerateErrorLatencyDistribution(percentiles histwriter.Percentiles, file string) error {
	return generateLatencyDistribution(s.ErrorHistogram, s.UncorrectedErrorHistogram, s.RequestRate, percentiles, file)
}

func getOneByPercentile(percentile float64) float64 {
	if percentile < 100 {
		return 1 / (1 - (percentile / 100))
	}
	return float64(10000000)
}

func generateLatencyDistribution(histogram, unHistogram *hdrhistogram.Histogram, requestRate uint64, percentiles histwriter.Percentiles, file string) error {
	scaleFactor := 0.000001 // Scale ns to ms.
	if err := histwriter.WriteDistributionFile(histogram, percentiles, scaleFactor, file); err != nil {
		return err
	}

	// Generate uncorrected distribution.
	if requestRate > 0 {
		if err := histwriter.WriteDistributionFile(unHistogram, percentiles, scaleFactor, fmt.Sprintf("uncorrected_%s", file)); err != nil {
			return err
		}
	}

	return nil
}

// merge the other Summary into this one.
func (s *Summary) merge(o *Summary) {
	if o.TimeElapsed > s.TimeElapsed {
		s.TimeElapsed = o.TimeElapsed
	}
	s.SuccessHistogram.Merge(o.SuccessHistogram)
	s.UncorrectedSuccessHistogram.Merge(o.UncorrectedSuccessHistogram)
	s.ErrorHistogram.Merge(o.ErrorHistogram)
	s.UncorrectedErrorHistogram.Merge(o.UncorrectedErrorHistogram)
	s.SuccessTotal += o.SuccessTotal
	s.ErrorTotal += o.ErrorTotal
	s.Throughput += o.Throughput
	s.RequestRate += o.RequestRate
}
