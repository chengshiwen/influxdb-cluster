package httputil

import (
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/influxdata/influxdb/pkg/jwtutil"
)

const (
	AuthTypeNone  = "none"
	AuthTypeBasic = "basic"
	AuthTypeJWT   = "jwt"
	JWTExpiration = 5 * time.Minute
)

type Config struct {
	AuthEnabled bool
	AuthType    string
	Username    string
	Password    string
	Secret      string
	UserAgent   string
	UseTLS      bool
	SkipTLS     bool
}

type Client struct {
	httpClient  *http.Client
	authEnabled bool
	authType    string
	username    string
	password    string
	secret      string
	userAgent   string
}

func NewClient(c Config) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if c.UseTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: c.SkipTLS}
	}
	return &Client{
		httpClient: &http.Client{
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return errors.New("stopped after 10 redirects")
				}
				if len(via) > 0 {
					if err := SetHeaderAuth(req, c.AuthEnabled, c.AuthType, c.Username, c.Password, c.Secret, c.UserAgent); err != nil {
						return err
					}
				}
				return nil
			},
		},
		authEnabled: c.AuthEnabled,
		authType:    c.AuthType,
		username:    c.Username,
		password:    c.Password,
		secret:      c.Secret,
		userAgent:   c.UserAgent,
	}
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if err := SetHeaderAuth(req, c.authEnabled, c.authType, c.username, c.password, c.secret, c.userAgent); err != nil {
		return nil, err
	}
	return c.httpClient.Do(req)
}

func (c *Client) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

func (c *Client) Post(url, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	return c.Do(req)
}

func (c *Client) PostJSON(url string, body io.Reader) (*http.Response, error) {
	return c.Post(url, "application/json", body)
}

func (c *Client) PostForm(url string, data url.Values) (*http.Response, error) {
	return c.Post(url, "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
}

func (c *Client) PostEmpty(url string) (*http.Response, error) {
	return c.Post(url, "", nil)
}

func (c *Client) Head(url string) (resp *http.Response, err error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

func (c *Client) CloseIdleConnections() {
	type closeIdler interface {
		CloseIdleConnections()
	}
	if tr, ok := c.httpClient.Transport.(closeIdler); ok {
		tr.CloseIdleConnections()
	}
}

func SetHeaderAuth(req *http.Request, authEnabled bool, authType, username, password, secret, userAgent string) error {
	req.Header.Set("User-Agent", userAgent)
	if authEnabled {
		authType = strings.ToLower(authType)
		if authType == AuthTypeBasic {
			req.SetBasicAuth(username, password)
		} else if authType == AuthTypeJWT {
			signed, err := jwtutil.SignedString(username, secret, JWTExpiration)
			if err != nil {
				return err
			}
			req.Header.Set("Authorization", "Bearer "+signed)
		}
	}
	return nil
}
