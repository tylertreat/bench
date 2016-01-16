package requester

import (
	"errors"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/tylertreat/bench"
)

// KafkaRequesterFactory implements RequesterFactory by creating a Requester
// which publishes messages to Kafka and waits to consume them.
type KafkaRequesterFactory struct {
	URLs        []string
	PayloadSize int
	Topic       string
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (k *KafkaRequesterFactory) GetRequester(num uint64) bench.Requester {
	return &kafkaRequester{
		urls:        k.URLs,
		payloadSize: k.PayloadSize,
		topic:       k.Topic + "-" + strconv.FormatUint(num, 10),
	}
}

// kafkaRequester implements Requester by publishing a message to Kafka and
// waiting to consume it.
type kafkaRequester struct {
	urls              []string
	payloadSize       int
	topic             string
	producer          sarama.AsyncProducer
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	msg               *sarama.ProducerMessage
}

// Setup prepares the Requester for benchmarking.
func (k *kafkaRequester) Setup() error {
	config := sarama.NewConfig()
	producer, err := sarama.NewAsyncProducer(k.urls, config)
	if err != nil {
		return err
	}

	consumer, err := sarama.NewConsumer(k.urls, nil)
	if err != nil {
		producer.Close()
		return err
	}
	partitionConsumer, err := consumer.ConsumePartition(k.topic, 0, sarama.OffsetNewest)
	if err != nil {
		producer.Close()
		consumer.Close()
		return err
	}

	k.producer = producer
	k.consumer = consumer
	k.partitionConsumer = partitionConsumer
	k.msg = &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.ByteEncoder(make([]byte, k.payloadSize)),
	}

	return nil
}

// Request performs a synchronous request to the system under test.
func (k *kafkaRequester) Request() error {
	k.producer.Input() <- k.msg
	select {
	case <-k.partitionConsumer.Messages():
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("requester: Request timed out receiving")
	}
}

// Teardown is called upon benchmark completion.
func (k *kafkaRequester) Teardown() error {
	if err := k.partitionConsumer.Close(); err != nil {
		return err
	}
	if err := k.consumer.Close(); err != nil {
		return err
	}
	if err := k.producer.Close(); err != nil {
		return err
	}
	k.partitionConsumer = nil
	k.consumer = nil
	k.producer = nil
	return nil
}
