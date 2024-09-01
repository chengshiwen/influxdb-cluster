// Package announcer provides the announcer service.
package announcer // import "github.com/influxdata/influxdb/services/announcer"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/httputil"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// Service represents the announcer service.
type Service struct {
	Version string

	Server interface {
		HTTPAddr() string
		HTTPScheme() string
		TCPAddr() string
	}

	MetaClient interface {
		MetaServers() []string
	}

	client *httputil.Client
	config *meta.Config
	wg     sync.WaitGroup
	done   chan struct{}

	logger *zap.Logger
}

// NewService returns a configured announcer service.
func NewService(c *meta.Config) *Service {
	return &Service{
		config: c,
		logger: zap.NewNop(),
		client: httputil.NewClient(httputil.Config{
			AuthEnabled: c.MetaAuthEnabled,
			AuthType:    httputil.AuthTypeJWT,
			Secret:      c.MetaInternalSharedSecret,
			UserAgent:   "InfluxDB DataNode",
			UseTLS:      c.MetaTLSEnabled,
			SkipTLS:     c.MetaInsecureTLS,
		}),
	}
}

// Open starts announcer.
func (s *Service) Open() error {
	if s.done != nil {
		return nil
	}

	s.done = make(chan struct{})

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.run() }()
	return nil
}

// Close stops announcer.
func (s *Service) Close() error {
	if s.done == nil {
		return nil
	}

	s.logger.Info("Closing announcer service")
	s.client.CloseIdleConnections()
	close(s.done)

	s.wg.Wait()
	s.done = nil
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.logger = log.With(zap.String("service", "announcer"))
}

func (s *Service) run() {
	ticker := time.NewTicker(time.Duration(s.config.GossipFrequency))
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return

		case <-ticker.C:
			metaServers := s.MetaClient.MetaServers()
			if len(metaServers) == 0 {
				continue
			}
			announcement := &meta.Announcement{
				TCPAddr:    s.Server.TCPAddr(),
				HTTPAddr:   s.Server.HTTPAddr(),
				HTTPScheme: s.Server.HTTPScheme(),
				Time:       time.Time{},
				NodeType:   meta.NodeTypeData,
				Status:     meta.NodeStatusJoined,
				Context:    nil,
				Version:    s.Version,
			}
			data, _ := json.Marshal(announcement)
			for i := 0; i < len(metaServers); i++ {
				uri := fmt.Sprintf("%s://%s/announce", s.metaHTTPScheme(), metaServers[i])
				resp, err := s.client.PostJSON(uri, bytes.NewBuffer(data))
				if err != nil {
					continue
				}
				resp.Body.Close()
				break
			}
		}
	}
}

func (s *Service) metaHTTPScheme() string {
	if s.config.MetaTLSEnabled {
		return "https"
	}
	return "http"
}
