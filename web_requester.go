package bench

import "net/http"

// WebRequester implements Requester by making a GET request to the provided
// URL.
type WebRequester struct {
	URL string
}

// Setup prepares the Requester for benchmarking.
func (w *WebRequester) Setup() error { return nil }

// Request performs a synchronous request to the system under test.
func (w *WebRequester) Request() error {
	resp, err := http.Get(w.URL)
	resp.Body.Close()
	return err
}

// Teardown is called upon benchmark completion.
func (w *WebRequester) Teardown() error { return nil }
