package coordinator

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"google.golang.org/grpc/metadata"
)

const (
	storeReadResponseMessage byte = iota + 1
	storeTrailerMetadataMessage
)

// storeStreamSender is a sender for encoding store to w.
type storeStreamSender struct {
	w  io.Writer
	md metadata.MD
}

// NewStoreStreamSender encodes store to w.
func NewStoreStreamSender(w io.Writer) *storeStreamSender {
	return &storeStreamSender{
		w: w,
	}
}

func (s *storeStreamSender) Send(rr *datatypes.ReadResponse) error {
	if rr == nil {
		return nil
	}
	buf, err := rr.Marshal()
	if err != nil {
		return err
	}
	return WriteTLV(s.w, storeReadResponseMessage, buf)
}

func (s *storeStreamSender) SetTrailer(md metadata.MD) {
	s.md = md
}

func (s *storeStreamSender) Close() error {
	if s.md.Len() > 0 {
		buf, err := json.Marshal(&s.md)
		if err != nil {
			return err
		}
		if err := WriteTLV(s.w, storeTrailerMetadataMessage, buf); err != nil {
			return err
		}
	}
	return nil
}

// storeStreamReceiver is a receiver for decoding store from r.
type storeStreamReceiver struct {
	r     io.Reader
	stats cursors.CursorStats
}

// NewStoreStreamReceiver decodes store from r.
func NewStoreStreamReceiver(r io.Reader) *storeStreamReceiver {
	return &storeStreamReceiver{
		r: r,
	}
}

func (s *storeStreamReceiver) Recv() (*datatypes.ReadResponse, error) {
	typ, buf, err := ReadTLV(s.r)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "use of closed network connection") {
			return nil, io.EOF
		}
		return nil, err
	}
	switch typ {
	case storeTrailerMetadataMessage:
		var md metadata.MD
		if err := json.Unmarshal(buf, &md); err != nil {
			return nil, err
		}
		if bytes := md.Get("scanned-bytes"); len(bytes) > 0 {
			s.stats.ScannedBytes, _ = strconv.Atoi(bytes[len(bytes)-1])
		}
		if values := md.Get("scanned-values"); len(values) > 0 {
			s.stats.ScannedValues, _ = strconv.Atoi(values[len(values)-1])
		}
		return nil, nil
	case storeReadResponseMessage:
		fallthrough
	default:
		rr := &datatypes.ReadResponse{}
		if err := rr.Unmarshal(buf); err != nil {
			return nil, err
		}
		return rr, nil
	}
}

func (s *storeStreamReceiver) Stats() cursors.CursorStats {
	return s.stats
}

func (s *storeStreamReceiver) Close() error {
	if r, ok := s.r.(io.ReadCloser); ok {
		return r.Close()
	}
	return nil
}
