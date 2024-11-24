package messages

import (
	"bytes"
	"encoding/binary"
)

type Request struct {
	Size          int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
}

func (r *Request) IsSupportedVersion(min, max int16) bool {
	return min <= r.ApiVersion &&
		r.ApiVersion <= max
}

func DeserializeRequest(b bytes.Buffer) *Request {
	r := Request{}
	reader := bytes.NewReader(b.Bytes())
	binary.Read(reader, binary.BigEndian, &r)
	return &r
}
