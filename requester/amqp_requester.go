package requester

import (
	"errors"
	"strconv"
	"time"

	"github.com/streadway/amqp"
	"github.com/tylertreat/bench"
)

// AMQPRequesterFactory implements RequesterFactory by creating a Requester
// which publishes messages to an AMQP exchange and waits to consume them.
type AMQPRequesterFactory struct {
	URL         string
	PayloadSize int
	Queue       string
	Exchange    string
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (r *AMQPRequesterFactory) GetRequester(num uint64) bench.Requester {
	return &amqpRequester{
		url:          r.URL,
		payloadSize:  r.PayloadSize,
		queueName:    r.Queue + "-" + strconv.FormatUint(num, 10),
		exchangeName: r.Exchange + "-" + strconv.FormatUint(num, 10),
	}
}

// amqpRequester implements Requester by publishing a message to an AMQP
// exhcnage and waiting to consume it.
type amqpRequester struct {
	url          string
	payloadSize  int
	queueName    string
	exchangeName string
	conn         *amqp.Connection
	queue        amqp.Queue
	channel      *amqp.Channel
	inbound      <-chan amqp.Delivery
	msg          amqp.Publishing
}

// Setup prepares the Requester for benchmarking.
func (r *amqpRequester) Setup() error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return err
	}
	c, err := conn.Channel()
	if err != nil {
		return err
	}
	queue, err := c.QueueDeclare(
		r.queueName, // name
		false,       // not durable
		false,       // delete when unused
		true,        // exclusive
		false,       // no wait
		nil,         // arguments
	)
	if err != nil {
		return err
	}
	err = c.ExchangeDeclare(
		r.exchangeName, // name
		"fanout",       // type
		false,          // not durable
		false,          // auto-deleted
		false,          // internal
		false,          // no wait
		nil,            // arguments
	)
	if err != nil {
		return err
	}
	err = c.QueueBind(
		r.queueName,
		r.queueName,
		r.exchangeName,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	inbound, err := c.Consume(
		r.queueName, // queue
		"",          // consumer
		true,        // auto ack
		false,       // exclusive
		true,        // no local
		false,       // no wait
		nil,         // args
	)
	if err != nil {
		return err
	}
	r.conn = conn
	r.queue = queue
	r.channel = c
	r.inbound = inbound
	r.msg = amqp.Publishing{
		DeliveryMode: amqp.Transient,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         make([]byte, r.payloadSize),
	}
	return nil
}

// Request performs a synchronous request to the system under test.
func (r *amqpRequester) Request() error {
	if err := r.channel.Publish(
		r.exchangeName, // exchange
		"",             // routing key
		false,          // mandatory
		false,          // immediate
		r.msg,
	); err != nil {
		return err
	}
	select {
	case <-r.inbound:
	case <-time.After(30 * time.Second):
		return errors.New("requester: Request timed out receiving")
	}
	return nil
}

// Teardown is called upon benchmark completion.
func (r *amqpRequester) Teardown() error {
	if err := r.channel.Close(); err != nil {
		return err
	}
	return r.conn.Close()
}
