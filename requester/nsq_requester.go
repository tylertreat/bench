package requester

import (
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/tylertreat/bench"
)

// NSQRequesterFactory implements RequesterFactory by creating a Requester
// which publishes messages to NSQ and waits to receive them.
type NSQRequesterFactory struct {
	URL         string
	PayloadSize int
	Topic       string
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (n *NSQRequesterFactory) GetRequester(num uint64) bench.Requester {
	return &nsqRequester{
		url:         n.URL,
		payloadSize: n.PayloadSize,
		topic:       n.Topic + "-" + strconv.FormatUint(num, 10),
		channel:     n.Topic + "-" + strconv.FormatUint(num, 10),
	}
}

// nsqRequester implements Requester by publishing a message to NSQ and
// waiting to receive it.
type nsqRequester struct {
	url         string
	payloadSize int
	topic       string
	channel     string
	producer    *nsq.Producer
	consumer    *nsq.Consumer
	msg         []byte
	msgChan     chan []byte
}

// Setup prepares the Requester for benchmarking.
func (n *nsqRequester) Setup() error {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(n.url, config)
	if err != nil {
		return err
	}
	consumer, err := nsq.NewConsumer(n.topic, n.channel, config)
	if err != nil {
		return err
	}
	n.msgChan = make(chan []byte)
	consumer.AddConcurrentHandlers(nsq.HandlerFunc(func(m *nsq.Message) error {
		m.Finish()
		n.msgChan <- m.Body
		return nil
	}), 1)
	if err := consumer.ConnectToNSQD(n.url); err != nil {
		producer.Stop()
		return err
	}
	n.producer = producer
	n.consumer = consumer
	n.msg = make([]byte, n.payloadSize)
	for i := 0; i < n.payloadSize; i++ {
		n.msg[i] = 'A' + uint8(rand.Intn(26))
	}
	return nil
}

// Request performs a synchronous request to the system under test.
func (n *nsqRequester) Request() error {
	if err := n.producer.Publish(n.topic, n.msg); err != nil {
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
func (n *nsqRequester) Teardown() error {
	if err := n.consumer.DisconnectFromNSQD(n.url); err != nil {
		return err
	}
	n.producer.Stop()

	n.consumer = nil
	n.producer = nil
	return nil
}
