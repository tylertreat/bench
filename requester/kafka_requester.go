package requester

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
)

// KafkaRequester implements Requester by publishing a message to Kafka and
// waiting to consume it.
type KafkaRequester struct {
	URL               string
	PayloadSize       int
	Topic             string
	producer          sarama.SyncProducer
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	msg               *sarama.ProducerMessage
}

// Setup prepares the Requester for benchmarking.
func (k *KafkaRequester) Setup() error {
	config := sarama.NewConfig()
	producer, err := sarama.NewSyncProducer([]string{k.URL}, config)
	if err != nil {
		return err
	}

	consumer, err := sarama.NewConsumer([]string{k.URL}, nil)
	if err != nil {
		producer.Close()
		return err
	}
	partitionConsumer, err := consumer.ConsumePartition(k.Topic, 0, sarama.OffsetNewest)
	if err != nil {
		producer.Close()
		consumer.Close()
		return err
	}

	k.producer = producer
	k.consumer = consumer
	k.partitionConsumer = partitionConsumer
	k.msg = &sarama.ProducerMessage{
		Topic: k.Topic,
		Value: sarama.ByteEncoder(make([]byte, k.PayloadSize)),
	}
	return nil
}

// Request performs a synchronous request to the system under test.
func (k *KafkaRequester) Request() error {
	_, _, err := k.producer.SendMessage(k.msg)
	if err != nil {
		return err
	}

	select {
	case <-k.partitionConsumer.Messages():
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("requester: Request timed out receiving")
	}
}

// Teardown is called upon benchmark completion.
func (k *KafkaRequester) Teardown() error {
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
