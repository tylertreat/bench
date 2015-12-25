package bench

import (
	"fmt"
	"os"
	"time"

	"github.com/codahale/hdrhistogram"
)

// Summary contains the results of a Benchmark run.
type Summary struct {
	Connections          uint64
	RequestRate          uint64
	TotalRequests        uint64
	TimeElapsed          time.Duration
	Histogram            *hdrhistogram.Histogram
	UncorrectedHistogram *hdrhistogram.Histogram
	Throughput           float64
}

// String returns a stringified version of the Summary.
func (s *Summary) String() string {
	return fmt.Sprintf(
		"{Connections: %d, RequestRate: %d, TotalRequests: %d, TimeElapsed: %s, Throughput: %.2f/s}",
		s.Connections, s.RequestRate, s.TotalRequests, s.TimeElapsed, s.Throughput)
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
		value := float64(s.Histogram.ValueAtQuantile(percentile)) / 1000000
		_, err := f.WriteString(fmt.Sprintf("%f    %f        %d            %f\n",
			value, percentile/100, 0, 1/(1-(percentile/100))))
		if err != nil {
			return err
		}
	}

	// Generate uncorrected distribution.
	if s.RequestRate > 0 {
		f, err := os.Create("uncorrected_" + file)
		if err != nil {
			return err
		}
		defer f.Close()

		f.WriteString("Value    Percentile    TotalCount    1/(1-Percentile)\n\n")
		for _, percentile := range percentiles {
			value := float64(s.UncorrectedHistogram.ValueAtQuantile(percentile)) / 1000000
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
	s.Histogram.Merge(o.Histogram)
	s.UncorrectedHistogram.Merge(o.UncorrectedHistogram)
	s.TotalRequests += o.TotalRequests
	s.Throughput += o.Throughput
	s.RequestRate += o.RequestRate
}
