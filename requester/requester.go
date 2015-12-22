package requester

// Requester synchronously issues requests for a particular system under test.
type Requester interface {
	// Setup prepares the Requester for benchmarking.
	Setup() error

	// Request performs a synchronous request to the system under test.
	Request() error

	// Teardown is called upon benchmark completion.
	Teardown() error
}
