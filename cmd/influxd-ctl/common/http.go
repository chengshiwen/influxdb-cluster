package common

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type HTTPClient struct {
	client *http.Client
	cOpts  *Options
}

func NewHTTPClient(cOpts *Options) *HTTPClient {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: cOpts.SkipTLS},
	}
	return &HTTPClient{
		client: &http.Client{Transport: transport},
		cOpts:  cOpts,
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
	return c.client.Post(c.fullURL(addr, path), "application/json", body)
}

func (c *HTTPClient) PostFormWithAddr(addr, path string, data url.Values) (*http.Response, error) {
	return c.client.PostForm(c.fullURL(addr, path), data)
}

func (c *HTTPClient) PostEmptyWithAddr(addr, path string) (*http.Response, error) {
	return c.client.Post(c.fullURL(addr, path), "", nil)
}

func (c *HTTPClient) RequestWithAddr(method, addr, path, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, c.fullURL(addr, path), body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return c.client.Do(req)
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

func (c *HTTPClient) Request(method, path, contentType string, body io.Reader) (*http.Response, error) {
	return c.RequestWithAddr(method, c.cOpts.BindAddr, path, contentType, body)
}
