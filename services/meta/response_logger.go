package meta

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxql"
)

// responseLogger is wrapper of http.ResponseWriter that keeps track of its HTTP status
// code and body size
type responseLogger struct {
	w      http.ResponseWriter
	status int
	size   int
}

func (l *responseLogger) CloseNotify() <-chan bool {
	if notifier, ok := l.w.(http.CloseNotifier); ok {
		return notifier.CloseNotify()
	}
	// needed for response recorder for testing
	return make(<-chan bool)
}

func (l *responseLogger) Header() http.Header {
	return l.w.Header()
}

func (l *responseLogger) Flush() {
	l.w.(http.Flusher).Flush()
}

func (l *responseLogger) Write(b []byte) (int, error) {
	if l.status == 0 {
		// Set status if WriteHeader has not been called
		l.status = http.StatusOK
	}

	size, err := l.w.Write(b)
	l.size += size
	return size, err
}

func (l *responseLogger) WriteHeader(s int) {
	l.w.WriteHeader(s)
	l.status = s
}

func (l *responseLogger) Status() int {
	if l.status == 0 {
		// This can happen if we never actually write data, but only set response headers.
		l.status = http.StatusOK
	}
	return l.status
}

func (l *responseLogger) Size() int {
	return l.size
}

// redact any occurrence of a password parameter, 'p'
func redactPassword(r *http.Request) {
	q := r.URL.Query()
	if p := q.Get("p"); p != "" {
		q.Set("p", "[REDACTED]")
		r.URL.RawQuery = q.Encode()
	}
}

// Common Log Format: http://en.wikipedia.org/wiki/Common_Log_Format

// buildLogLine creates a common log format
// in addition to the common fields, we also append referrer, user agent,
// request ID and response time (microseconds)
// ie, in apache mod_log_config terms:
//
//	%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"" %L %D
func buildLogLine(l *responseLogger, r *http.Request, start time.Time) string {

	redactPassword(r)

	username := parseUsername(r)

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	if xff := r.Header["X-Forwarded-For"]; xff != nil {
		addrs := append(xff, host)
		host = strings.Join(addrs, ",")
	}

	uri := r.URL.RequestURI()

	referer := r.Referer()

	userAgent := r.UserAgent()

	allKeyValues := make([]string, 0, len(r.PostForm))
	if r.Method == "POST" {
		for k, values := range r.PostForm {
			if k == "p" || k == "P" {
				// Note: if there are multiple "p" values, they are all replaced by a single "[REDACTED]".
				r.PostForm.Set(k, "[REDACTED]")
				values = r.PostForm[k]
			}
			valuesSlice := make([]string, 0, len(values))
			for _, v := range values {
				valuesSlice = append(valuesSlice, fmt.Sprintf("'%s'", v))
			}
			joined := strings.Join(valuesSlice, ", ")
			allKeyValues = append(allKeyValues, fmt.Sprintf("{'%s': %s}", k, joined))
		}

		return fmt.Sprintf(`%s - %s [%s] "%s %s %s %s" %s %s "%s" "%s" %s %d`,
			host,
			detect(username, "-"),
			start.Format("02/Jan/2006:15:04:05 -0700"),
			r.Method,
			uri,
			r.Proto,
			strings.Join(allKeyValues, ", "),
			detect(strconv.Itoa(l.Status()), "-"),
			strconv.Itoa(l.Size()),
			detect(referer, "-"),
			detect(userAgent, "-"),
			r.Header.Get("Request-Id"),
			// response time, report in microseconds because this is consistent
			// with apache's %D parameter in mod_log_config
			int64(time.Since(start)/time.Microsecond))

	} else {
		return fmt.Sprintf(`%s - %s [%s] "%s %s %s" %s %s "%s" "%s" %s %d`,
			host,
			detect(username, "-"),
			start.Format("02/Jan/2006:15:04:05 -0700"),
			r.Method,
			uri,
			r.Proto,
			detect(strconv.Itoa(l.Status()), "-"),
			strconv.Itoa(l.Size()),
			detect(referer, "-"),
			detect(userAgent, "-"),
			r.Header.Get("Request-Id"),
			// response time, report in microseconds because this is consistent
			// with apache's %D parameter in mod_log_config
			int64(time.Since(start)/time.Microsecond))
	}
}

// detect detects the first presence of a non blank string and returns it
func detect(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// parses the username either from the url or auth header
func parseUsername(r *http.Request) string {
	var (
		username = ""
		url      = r.URL
	)

	// get username from the url if passed there
	if url.User != nil {
		if name := url.User.Username(); name != "" {
			username = name
		}
	}

	// Try to get the username from the query param 'u'
	q := url.Query()
	if u := q.Get("u"); u != "" {
		username = u
	}

	// Try to get it from the authorization header if set there
	if username == "" {
		if u, _, ok := r.BasicAuth(); ok {
			username = u
		}
	}
	return username
}

// sanitize redacts passwords from query string for logging.
func sanitize(r *http.Request) {
	values := r.URL.Query()
	for i, q := range values["q"] {
		values["q"][i] = influxql.Sanitize(q)
	}
	r.URL.RawQuery = values.Encode()
}
