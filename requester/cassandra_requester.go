package requester

import (
	"github.com/gocql/gocql"
	"github.com/tylertreat/bench"
)

// CassandraRequesterFactory implements RequesterFactory by creating a
// Requester which issues queries to Cassandra. Works with Cassandra 2.x.x.
type CassandraRequesterFactory struct {
	URLs        []string
	Keyspace    string
	Consistency gocql.Consistency
	Statement   string
	Values      []interface{}
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (c *CassandraRequesterFactory) GetRequester(uint64) bench.Requester {
	return &cassandraRequester{
		urls:        c.URLs,
		keyspace:    c.Keyspace,
		consistency: c.Consistency,
		statement:   c.Statement,
		values:      c.Values,
	}
}

// cassandraRequester implements Requester by issuing a query to Cassandra.
// Works with Cassandra 2.x.x.
type cassandraRequester struct {
	urls        []string
	keyspace    string
	consistency gocql.Consistency
	statement   string
	values      []interface{}
	session     *gocql.Session
}

// Setup prepares the Requester for benchmarking.
func (c *cassandraRequester) Setup() error {
	cluster := gocql.NewCluster(c.urls...)
	cluster.Keyspace = c.keyspace
	cluster.Consistency = c.consistency
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	c.session = session
	return nil
}

// Request performs a synchronous request to the system under test.
func (c *cassandraRequester) Request() error {
	return c.session.Query(c.statement, c.values...).Exec()
}

// Teardown is called upon benchmark completion.
func (c *cassandraRequester) Teardown() error {
	c.session.Close()
	c.session = nil
	return nil
}
