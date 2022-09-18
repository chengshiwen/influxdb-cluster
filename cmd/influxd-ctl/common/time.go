package common

import (
	"time"
)

func FormatRFC3339(t time.Time) string {
	if !t.IsZero() {
		return t.UTC().Format(time.RFC3339)
	}
	return ""
}

func FormatRFC3339Nano(t time.Time) string {
	if !t.IsZero() {
		return t.UTC().Format(time.RFC3339Nano)
	}
	return ""
}
