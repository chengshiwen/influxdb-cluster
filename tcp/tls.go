package tcp

import (
	"crypto/tls"
	"net"
	"time"
)

// TLSConfig returns a TLS config.
func TLSConfig(tlsConfig *tls.Config, useTLS bool, certFile, keyFile string) (*tls.Config, error) {
	if useTLS {
		if keyFile == "" {
			keyFile = certFile
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		var config *tls.Config
		if tlsConfig != nil {
			config = tlsConfig.Clone()
		} else {
			config = new(tls.Config)
		}
		config.Certificates = []tls.Certificate{cert}
		return config, nil
	}
	return nil, nil
}

// TLSClientConfig returns a client TLS config.
func TLSClientConfig(useTLS bool, skipTLS bool) *tls.Config {
	if useTLS {
		return &tls.Config{InsecureSkipVerify: skipTLS}
	}
	return nil
}

// ListenTLS creates a listener accepting connections on the given network address and tls config.
func ListenTLS(network, address string, tlsConfig *tls.Config) (net.Listener, error) {
	if tlsConfig != nil {
		return tls.Listen(network, address, tlsConfig)
	}
	return net.Listen(network, address)
}

// DialTLS connects to a remote mux listener with a given tls.
func DialTLS(network, address string, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig != nil {
		return tls.Dial(network, address, tlsConfig)
	}
	return net.Dial(network, address)
}

// DialTLSTimeout acts like DialTLS but takes a timeout.
func DialTLSTimeout(network, address string, tlsConfig *tls.Config, timeout time.Duration) (net.Conn, error) {
	if tlsConfig != nil {
		return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, network, address, tlsConfig)
	}
	return net.DialTimeout(network, address, timeout)
}

// DialTLSHeader connects to a remote mux listener with a given tls config and header byte.
func DialTLSHeader(network, address string, tlsConfig *tls.Config, header byte) (net.Conn, error) {
	conn, err := DialTLS(network, address, tlsConfig)
	if err != nil {
		return nil, err
	}
	return WriteHeader(conn, header)
}

// DialTLSTimeoutHeader acts like DialTLSHeader but takes a timeout.
func DialTLSTimeoutHeader(network, address string, tlsConfig *tls.Config, timeout time.Duration, header byte) (net.Conn, error) {
	conn, err := DialTLSTimeout(network, address, tlsConfig, timeout)
	if err != nil {
		return nil, err
	}
	return WriteHeader(conn, header)
}
