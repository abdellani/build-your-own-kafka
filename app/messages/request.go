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
	// 	supportedVersions := map[int]struct {
	// 		MinVersion int16
	// 		MaxVersion int16
	// 	}{
	// 		18: {
	// 			MinVersion: 0,
	// 			MaxVersion: 4,
	// 		},
	// 	}
	// //	supportedVersion := supportedVersions[int(r.ApiKey)]
	return min <= r.ApiVersion &&
		r.ApiVersion <= max
}

func DeserializeRequest(b bytes.Buffer) *Request {
	r := Request{}
	reader := bytes.NewReader(b.Bytes())
	binary.Read(reader, binary.BigEndian, &r)
	return &r
}
