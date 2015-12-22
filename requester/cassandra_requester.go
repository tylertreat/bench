package requester

import "github.com/gocql/gocql"

// CassandraRequester implements Requester by issuing a query to Cassandra.
// Works with Cassandra 2.x.x.
type CassandraRequester struct {
	URLs        []string
	Keyspace    string
	Consistency gocql.Consistency
	Statement   string
	Values      []interface{}
	session     *gocql.Session
}

// Setup prepares the Requester for benchmarking.
func (c *CassandraRequester) Setup() error {
	cluster := gocql.NewCluster(c.URLs...)
	cluster.Keyspace = c.Keyspace
	cluster.Consistency = c.Consistency
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	c.session = session
	return nil
}

// Request performs a synchronous request to the system under test.
func (c *CassandraRequester) Request() error {
	return c.session.Query(c.Statement, c.Values...).Exec()
}

// Teardown is called upon benchmark completion.
func (c *CassandraRequester) Teardown() error {
	c.session.Close()
	c.session = nil
	return nil
}
