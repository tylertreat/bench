package requester

import (
	"strconv"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/tylertreat/bench"
)

// NATSRequesterFactory implements RequesterFactory by creating a Requester
// which publishes messages to NATS and waits to receive them.
type NATSRequesterFactory struct {
	URL         string
	PayloadSize int
	Subject     string
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (n *NATSRequesterFactory) GetRequester(num uint64) bench.Requester {
	return &natsRequester{
		url:         n.URL,
		payloadSize: n.PayloadSize,
		subject:     n.Subject + "-" + strconv.FormatUint(num, 10),
	}
}

// natsRequester implements Requester by publishing a message to NATS and
// waiting to receive it.
type natsRequester struct {
	url         string
	payloadSize int
	subject     string
	conn        *nats.Conn
	sub         *nats.Subscription
	msg         []byte
}

// Setup prepares the Requester for benchmarking.
func (n *natsRequester) Setup() error {
	conn, err := nats.Connect(n.url)
	if err != nil {
		return err
	}
	sub, err := conn.SubscribeSync(n.subject)
	if err != nil {
		conn.Close()
		return err
	}
	n.conn = conn
	n.sub = sub
	n.msg = make([]byte, n.payloadSize)
	return nil
}

// Request performs a synchronous request to the system under test.
func (n *natsRequester) Request() error {
	if err := n.conn.Publish(n.subject, n.msg); err != nil {
		return err
	}
	_, err := n.sub.NextMsg(30 * time.Second)
	return err
}

// Teardown is called upon benchmark completion.
func (n *natsRequester) Teardown() error {
	if err := n.sub.Unsubscribe(); err != nil {
		return err
	}
	n.sub = nil
	n.conn.Close()
	n.conn = nil
	return nil
}
