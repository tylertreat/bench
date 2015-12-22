package requester

import (
	"time"

	"github.com/nats-io/nats"
)

// NATSRequester implements Requester by publishing a message to NATS and
// waiting to receive it.
type NATSRequester struct {
	URL         string
	PayloadSize int
	Subject     string
	conn        *nats.Conn
	sub         *nats.Subscription
	msg         []byte
}

// Setup prepares the Requester for benchmarking.
func (n *NATSRequester) Setup() error {
	conn, err := nats.Connect(n.URL)
	if err != nil {
		return err
	}
	sub, err := conn.SubscribeSync(n.Subject)
	if err != nil {
		conn.Close()
		return err
	}
	n.conn = conn
	n.sub = sub
	n.msg = make([]byte, n.PayloadSize)
	return nil
}

// Request performs a synchronous request to the system under test.
func (n *NATSRequester) Request() error {
	if err := n.conn.Publish(n.Subject, n.msg); err != nil {
		return err
	}
	_, err := n.sub.NextMsg(30 * time.Second)
	return err
}

// Teardown is called upon benchmark completion.
func (n *NATSRequester) Teardown() error {
	if err := n.sub.Unsubscribe(); err != nil {
		return err
	}
	n.sub = nil
	n.conn.Close()
	n.conn = nil
	return nil
}
