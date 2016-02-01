package bench

import (
	"fmt"
	"os"
	"time"

	"github.com/codahale/hdrhistogram"
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
func (s *Summary) GenerateLatencyDistribution(percentiles Percentiles, file string) error {
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
func (s *Summary) GenerateErrorLatencyDistribution(percentiles Percentiles, file string) error {
	return generateLatencyDistribution(s.ErrorHistogram, s.UncorrectedErrorHistogram, s.RequestRate, percentiles, file)
}

func generateLatencyDistribution(histogram, unHistogram *hdrhistogram.Histogram, requestRate uint64, percentiles Percentiles, file string) error {
	if percentiles == nil {
		percentiles = Logarithmic
	}
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString("Value    Percentile    TotalCount    1/(1-Percentile)\n\n")
	for _, percentile := range percentiles {
		value := float64(histogram.ValueAtQuantile(percentile)) / 1000000
		_, err := f.WriteString(fmt.Sprintf("%f    %f        %d            %f\n",
			value, percentile/100, 0, 1/(1-(percentile/100))))
		if err != nil {
			return err
		}
	}

	// Generate uncorrected distribution.
	if requestRate > 0 {
		f, err := os.Create("uncorrected_" + file)
		if err != nil {
			return err
		}
		defer f.Close()

		f.WriteString("Value    Percentile    TotalCount    1/(1-Percentile)\n\n")
		for _, percentile := range percentiles {
			value := float64(unHistogram.ValueAtQuantile(percentile)) / 1000000
			_, err := f.WriteString(fmt.Sprintf("%f    %f        %d            %f\n",
				value, percentile/100, 0, 1/(1-(percentile/100))))
			if err != nil {
				return err
			}
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
