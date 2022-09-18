package common

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/pkg/httputil"
)

type HTTPClient struct {
	client *httputil.Client
	cOpts  *Options
}

func NewHTTPClient(cOpts *Options) *HTTPClient {
	return &HTTPClient{
		client: httputil.NewClient(httputil.Config{
			AuthEnabled: cOpts.AuthType != httputil.AuthTypeNone,
			AuthType:    cOpts.AuthType,
			Username:    cOpts.Username,
			Password:    cOpts.Password,
			Secret:      cOpts.Secret,
			UserAgent:   "InfluxDB Cluster Client",
			UseTLS:      cOpts.BindTLS,
			SkipTLS:     cOpts.SkipTLS,
		}),
		cOpts: cOpts,
	}
}

func (c *HTTPClient) scheme() string {
	if c.cOpts.BindTLS {
		return "https"
	}
	return "http"
}

func (c *HTTPClient) fullURL(addr, path string) string {
	return fmt.Sprintf("%s://%s%s", c.scheme(), addr, path)
}

func (c *HTTPClient) GetWithAddr(addr, path string) (*http.Response, error) {
	return c.client.Get(c.fullURL(addr, path))
}

func (c *HTTPClient) PostWithAddr(addr, path, contentType string, body io.Reader) (*http.Response, error) {
	return c.client.Post(c.fullURL(addr, path), contentType, body)
}

func (c *HTTPClient) PostJSONWithAddr(addr, path string, body io.Reader) (*http.Response, error) {
	return c.client.PostJSON(c.fullURL(addr, path), body)
}

func (c *HTTPClient) PostFormWithAddr(addr, path string, data url.Values) (*http.Response, error) {
	return c.client.PostForm(c.fullURL(addr, path), data)
}

func (c *HTTPClient) PostEmptyWithAddr(addr, path string) (*http.Response, error) {
	return c.client.PostEmpty(c.fullURL(addr, path))
}

func (c *HTTPClient) Get(path string) (*http.Response, error) {
	return c.GetWithAddr(c.cOpts.BindAddr, path)
}

func (c *HTTPClient) Post(path, contentType string, body io.Reader) (*http.Response, error) {
	return c.PostWithAddr(c.cOpts.BindAddr, path, contentType, body)
}

func (c *HTTPClient) PostJSON(path string, body io.Reader) (*http.Response, error) {
	return c.PostJSONWithAddr(c.cOpts.BindAddr, path, body)
}

func (c *HTTPClient) PostForm(path string, data url.Values) (*http.Response, error) {
	return c.PostFormWithAddr(c.cOpts.BindAddr, path, data)
}

func (c *HTTPClient) PostEmpty(path string) (*http.Response, error) {
	return c.PostEmptyWithAddr(c.cOpts.BindAddr, path)
}

func (c *HTTPClient) Join(addr string, v interface{}) error {
	data := url.Values{"addr": {addr}}
	resp, err := c.PostForm("/join", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusOK(resp, v)
}

func (c *HTTPClient) JoinWithAddr(reqAddr, addr string, v interface{}) error {
	data := url.Values{"addr": {addr}}
	resp, err := c.PostFormWithAddr(reqAddr, "/join", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusOK(resp, v)
}

func (c *HTTPClient) Remove(addr string, force bool, tcpAddr string) error {
	data := url.Values{"httpAddr": {addr}}
	data.Set("force", strconv.FormatBool(force))
	data.Set("tcpAddr", tcpAddr)
	resp, err := c.PostForm("/remove", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusNoContent(resp)
}

func (c *HTTPClient) Leave(addr string) error {
	resp, err := c.PostEmptyWithAddr(addr, "/leave")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusNoContent(resp)
}

func (c *HTTPClient) AddData(addr string, v interface{}) error {
	data := url.Values{"addr": {addr}}
	resp, err := c.PostForm("/add-data", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusOK(resp, v)
}

func (c *HTTPClient) AddDataWithAddr(reqAddr, addr string, v interface{}) error {
	data := url.Values{"addr": {addr}}
	resp, err := c.PostFormWithAddr(reqAddr, "/add-data", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusOK(resp, v)
}

func (c *HTTPClient) RemoveData(addr string, force bool) error {
	data := url.Values{"addr": {addr}, "force": {strconv.FormatBool(force)}}
	resp, err := c.PostForm("/remove-data", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusNoContent(resp)
}

func (c *HTTPClient) RemoveDataWithAddr(reqAddr, addr string, force bool) error {
	data := url.Values{"addr": {addr}, "force": {strconv.FormatBool(force)}}
	resp, err := c.PostFormWithAddr(reqAddr, "/remove-data", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusNoContent(resp)
}

func (c *HTTPClient) UpdateData(oldAddr, newAddr string, v interface{}) error {
	data := url.Values{"oldAddr": {oldAddr}, "newAddr": {newAddr}}
	resp, err := c.PostForm("/update-data", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusOK(resp, v)
}

func (c *HTTPClient) ShowCluster(v interface{}) error {
	resp, err := c.Get("/show-cluster")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusOK(resp, v)
}

func (c *HTTPClient) ShowShards(verbose bool, v interface{}) error {
	path := "/show-shards"
	if verbose {
		path += "?verbose=true"
	}
	resp, err := c.Get(path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusOK(resp, v)
}

func (c *HTTPClient) CopyShard(srcAddr, destAddr string, shardID uint64) error {
	data := url.Values{"src": {srcAddr}, "dest": {destAddr}, "shard": {strconv.FormatUint(shardID, 10)}}
	resp, err := c.PostForm("/copy-shard", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusNoContent(resp)
}

func (c *HTTPClient) RemoveShard(srcAddr string, shardID uint64) error {
	data := url.Values{"src": {srcAddr}, "shard": {strconv.FormatUint(shardID, 10)}}
	resp, err := c.PostForm("/remove-shard", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusNoContent(resp)
}

func (c *HTTPClient) TruncateShards(delay time.Duration) error {
	data := url.Values{"delay": {delay.String()}}
	resp, err := c.PostForm("/truncate-shards", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusNoContent(resp)
}

func (c *HTTPClient) Status(addr string, v interface{}) error {
	resp, err := c.GetWithAddr(addr, "/status")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return parseStatusOK(resp, v)
}

func (c *HTTPClient) DataNodeExists(addr string) (bool, bool, []string) {
	var r struct {
		NodeType  string   `json:"nodeType"`
		MetaAddrs []string `json:"metaAddrs"`
	}
	if err := c.Status(addr, &r); err != nil {
		return false, false, nil
	}
	return r.NodeType == "data", len(r.MetaAddrs) > 0, r.MetaAddrs
}

func (c *HTTPClient) MetaNodeExists(addr string) (bool, bool, string) {
	var r struct {
		NodeType string `json:"nodeType"`
		Leader   string `json:"leader"`
	}
	if err := c.Status(addr, &r); err != nil {
		return false, false, ""
	}
	return r.NodeType == "meta", r.Leader != "", r.Leader
}

func (c *HTTPClient) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

func parseStatusOK(resp *http.Response, v interface{}) error {
	if resp.StatusCode != http.StatusOK {
		return DecodeError(resp.Body)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func parseStatusNoContent(resp *http.Response) error {
	if resp.StatusCode != http.StatusNoContent {
		return DecodeError(resp.Body)
	}
	return nil
}
