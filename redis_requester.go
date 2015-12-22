package bench

import "github.com/garyburd/redigo/redis"

// RedisRequester implements Requester by sending the configured command and
// arguments to Redis and waiting for the reply.
type RedisRequester struct {
	URL     string
	Command string
	Args    []interface{}
	conn    redis.Conn
}

// Setup prepares the Requester for benchmarking.
func (r *RedisRequester) Setup() error {
	conn, err := redis.Dial("tcp", r.URL)
	if err != nil {
		return err
	}
	r.conn = conn
	return nil
}

// Request performs a synchronous request to the system under test.
func (r *RedisRequester) Request() error {
	_, err := r.conn.Do(r.Command, r.Args...)
	return err
}

// Teardown is called upon benchmark completion.
func (r *RedisRequester) Teardown() error {
	if err := r.conn.Close(); err != nil {
		return err
	}
	r.conn = nil
	return nil
}

// RedisPubSubRequester implements Requester by publishing a message to Redis
// and waiting to receive it.
type RedisPubSubRequester struct {
	URL           string
	PayloadSize   int
	Channel       string
	publishConn   redis.Conn
	subscribeConn *redis.PubSubConn
	msg           string
}

// Setup prepares the Requester for benchmarking.
func (r *RedisPubSubRequester) Setup() error {
	pubConn, err := redis.Dial("tcp", r.URL)
	if err != nil {
		return err
	}
	subConn, err := redis.Dial("tcp", r.URL)
	if err != nil {
		return err
	}
	subscribeConn := &redis.PubSubConn{subConn}
	if err := subscribeConn.Subscribe(r.Channel); err != nil {
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
	r.msg = string(make([]byte, r.PayloadSize))
	return nil
}

// Request performs a synchronous request to the system under test.
func (r *RedisPubSubRequester) Request() error {
	if err := r.publishConn.Send("PUBLISH", r.Channel, r.msg); err != nil {
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
func (r *RedisPubSubRequester) Teardown() error {
	if err := r.publishConn.Close(); err != nil {
		return err
	}
	r.publishConn = nil
	if err := r.subscribeConn.Unsubscribe(r.Channel); err != nil {
		return err
	}
	if err := r.subscribeConn.Close(); err != nil {
		return err
	}
	r.subscribeConn = nil
	return nil
}
