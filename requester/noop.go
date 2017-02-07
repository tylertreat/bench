package requester

import (
	"sync"

	"github.com/tylertreat/bench"
)

type NOOPRequesterFactory struct {
}

var instance bench.Requester
var once sync.Once

func (n *NOOPRequesterFactory) GetRequester(num uint64) bench.Requester {
	once.Do(func() {
		instance = &noopRequester{}
	})
	return instance
}

type noopRequester struct {
}

func (n *noopRequester) Setup() error {
	return nil
}

func (n *noopRequester) Request() error {
	return nil
}

func (n *noopRequester) Teardown() error {
	return nil
}
