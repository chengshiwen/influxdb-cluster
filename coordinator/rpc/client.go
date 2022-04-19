package rpc

import (
	"net"

	"github.com/influxdata/influxdb/tcp"
)

// Client provides an API for the rpc service.
type Client struct {
	host string
}

// NewClient returns a new *Client.
func NewClient(host string) *Client {
	return &Client{host: host}
}

func (c *Client) dial() (net.Conn, error) {
	return tcp.Dial("tcp", c.host, MuxHeader)
}

func (c *Client) JoinCluster(metaServers []string, update bool) error {
	conn, err := c.dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send request.
	req := JoinClusterRequest{
		MetaServers: metaServers,
		Update:      update,
	}
	err = EncodeTLV(conn, JoinClusterRequestMessage, &req)
	if err != nil {
		return err
	}

	// Read the response.
	_, buf, err := ReadTLV(conn)
	if err != nil {
		return err
	}

	// Unmarshal response.
	var resp JoinClusterResponse
	if err = resp.UnmarshalBinary(buf); err != nil {
		return err
	}
	return resp.Err
}

func (c *Client) LeaveCluster() error {
	conn, err := c.dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send request.
	err = WriteType(conn, LeaveClusterRequestMessage)
	if err != nil {
		return err
	}

	// Read the response.
	_, buf, err := ReadTLV(conn)
	if err != nil {
		return err
	}

	// Unmarshal response.
	var resp LeaveClusterResponse
	if err = resp.UnmarshalBinary(buf); err != nil {
		return err
	}
	return resp.Err
}
