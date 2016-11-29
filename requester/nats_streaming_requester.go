package requester

import (
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/nats-io/go-nats-streaming"
	"github.com/tylertreat/bench"
)

// NATSStreamingRequesterFactory implements RequesterFactory by creating a
// Requester which publishes messages to NATS Streaming and waits to receive
// them.
type NATSStreamingRequesterFactory struct {
	PayloadSize int
	Subject     string
	ClientID    string
	URL         string
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (n *NATSStreamingRequesterFactory) GetRequester(num uint64) bench.Requester {
	return &natsStreamingRequester{
		url:         n.URL,
		clientID:    n.ClientID,
		payloadSize: n.PayloadSize,
		subject:     n.Subject + "-" + strconv.FormatUint(num, 10),
	}
}

// natsStreamingRequester implements Requester by publishing a message to NATS
// Streaming and waiting to receive it.
type natsStreamingRequester struct {
	url         string
	clientID    string
	payloadSize int
	subject     string
	conn        stan.Conn
	sub         stan.Subscription
	msg         []byte
	msgChan     chan []byte
}

// Setup prepares the Requester for benchmarking.
func (n *natsStreamingRequester) Setup() error {
	conn, err := stan.Connect("test-cluster", n.clientID, stan.NatsURL(n.url))
	if err != nil {
		return err
	}
	n.msgChan = make(chan []byte)
	sub, err := conn.Subscribe(n.subject, func(msg *stan.Msg) {
		n.msgChan <- msg.Data
	})
	if err != nil {
		conn.Close()
		return err
	}
	n.conn = conn
	n.sub = sub
	n.msg = make([]byte, n.payloadSize)
	for i := 0; i < n.payloadSize; i++ {
		n.msg[i] = 'A' + uint8(rand.Intn(26))
	}
	return nil
}

// Request performs a synchronous request to the system under test.
func (n *natsStreamingRequester) Request() error {
	if _, err := n.conn.PublishAsync(n.subject, n.msg, nil); err != nil {
		return err
	}
	select {
	case <-n.msgChan:
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("timeout")
	}
}

// Teardown is called upon benchmark completion.
func (n *natsStreamingRequester) Teardown() error {
	if err := n.sub.Unsubscribe(); err != nil {
		return err
	}
	n.sub = nil
	n.conn.Close()
	n.conn = nil
	return nil
}
