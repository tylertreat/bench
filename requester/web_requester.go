package requester

import (
	"net/http"

	"github.com/tylertreat/bench"
)

// WebRequesterFactory implements RequesterFactory by creating a Requester
// which makes GET requests to the provided URL.
type WebRequesterFactory struct {
	URL string
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (w *WebRequesterFactory) GetRequester(uint64) bench.Requester {
	return &webRequester{w.URL}
}

// webRequester implements Requester by making a GET request to the provided
// URL.
type webRequester struct {
	url string
}

// Setup prepares the Requester for benchmarking.
func (w *webRequester) Setup() error { return nil }

// Request performs a synchronous request to the system under test.
func (w *webRequester) Request() error {
	resp, err := http.Get(w.url)
	resp.Body.Close()
	return err
}

// Teardown is called upon benchmark completion.
func (w *webRequester) Teardown() error { return nil }
