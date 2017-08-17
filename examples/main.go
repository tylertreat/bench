package main

import (
	"fmt"
	"time"

	"github.com/tylertreat/bench"
	"github.com/tylertreat/bench/requester"
)

func main() {
	r := &requester.NATSRequesterFactory{
		URL:         "nats://localhost:4222",
		PayloadSize: 500,
		Subject:     "benchmark",
	}

	benchmark := bench.NewBenchmark(r, 10000, 1, 30*time.Second, 0)
	summary, err := benchmark.Run()
	if err != nil {
		panic(err)
	}

	fmt.Println(summary)
	summary.GenerateLatencyDistribution(nil, "nats.txt")
}
