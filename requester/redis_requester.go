package requester

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/tylertreat/bench"
)

// RedisRequesterFactory implements RequesterFactory by creating a Requester
// which sends the configured command and arguments to Redis and waits for the
// reply.
type RedisRequesterFactory struct {
	URL     string
	Command string
	Args    []interface{}
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (r *RedisRequesterFactory) GetRequester(uint64) bench.Requester {
	return &redisRequester{
		url:     r.URL,
		command: r.Command,
		args:    r.Args,
	}
}

// redisRequester implements Requester by sending the configured command and
// arguments to Redis and waiting for the reply.
type redisRequester struct {
	url     string
	command string
	args    []interface{}
	conn    redis.Conn
}

// Setup prepares the Requester for benchmarking.
func (r *redisRequester) Setup() error {
	conn, err := redis.Dial("tcp", r.url)
	if err != nil {
		return err
	}
	r.conn = conn
	return nil
}

// Request performs a synchronous request to the system under test.
func (r *redisRequester) Request() error {
	_, err := r.conn.Do(r.command, r.args...)
	return err
}

// Teardown is called upon benchmark completion.
func (r *redisRequester) Teardown() error {
	if err := r.conn.Close(); err != nil {
		return err
	}
	r.conn = nil
	return nil
}

// RedisPubSubRequesterFactory implements RequesterFactory by creating a
// Requester which publishes messages to Redis and waits to receive them.
type RedisPubSubRequesterFactory struct {
	URL         string
	PayloadSize int
	Channel     string
}

// redisPubSubRequester implements Requester by publishing a message to Redis
// and waiting to receive it.
type redisPubSubRequester struct {
	url           string
	payloadSize   int
	channel       string
	publishConn   redis.Conn
	subscribeConn *redis.PubSubConn
	msg           string
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (r *RedisPubSubRequesterFactory) GetRequester(num uint64) bench.Requester {
	return &redisPubSubRequester{
		url:         r.URL,
		payloadSize: r.PayloadSize,
		channel:     r.Channel + "-" + strconv.FormatUint(num, 10),
	}
}

// Setup prepares the Requester for benchmarking.
func (r *redisPubSubRequester) Setup() error {
	pubConn, err := redis.Dial("tcp", r.url)
	if err != nil {
		return err
	}
	subConn, err := redis.Dial("tcp", r.url)
	if err != nil {
		return err
	}
	subscribeConn := &redis.PubSubConn{subConn}
	if err := subscribeConn.Subscribe(r.channel); err != nil {
		subscribeConn.Close()
		return err
	}
	// Receive subscription message.
	switch recv := subscribeConn.Receive().(type) {
	case error:
		return recv
	}
	r.publishConn = pubConn
	r.subscribeConn = subscribeConn
	r.msg = string(make([]byte, r.payloadSize))
	return nil
}

// Request performs a synchronous request to the system under test.
func (r *redisPubSubRequester) Request() error {
	if err := r.publishConn.Send("PUBLISH", r.channel, r.msg); err != nil {
		return err
	}
	if err := r.publishConn.Flush(); err != nil {
		return err
	}
	switch recv := r.subscribeConn.Receive().(type) {
	case error:
		return recv
	default:
		return nil
	}
}

// Teardown is called upon benchmark completion.
func (r *redisPubSubRequester) Teardown() error {
	if err := r.publishConn.Close(); err != nil {
		return err
	}
	r.publishConn = nil
	if err := r.subscribeConn.Unsubscribe(r.channel); err != nil {
		return err
	}
	if err := r.subscribeConn.Close(); err != nil {
		return err
	}
	r.subscribeConn = nil
	return nil
}
