package messages

import (
	"bytes"
	"encoding/binary"
)

type DTPRequest struct {
	Size int32
	RequestHeaderV1
	Topics                 ARRAY[struct{ name COMPACT_STRING }]
	ResponsePartitionLimit int32
	Cursor                 struct {
		TopicName      COMPACT_STRING
		PartitionIndex int32
	}
	TAG_BUFFER
}

type RequestHeaderV0 struct {
	Size          int32 // Common
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
}

type RequestHeaderV1 struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      NULLABLE_STRING
}

type NULLABLE_STRING struct {
	N      int16 // null = -1
	String []byte
}

type COMPACT_STRING struct {
	N      UNSIGNED_VARINT // set to N+1
	String []byte
}

type UNSIGNED_VARINT []byte

type ARRAY[T any] struct {
	N     int32 // -1 means null
	Items []T
}

func (r *RequestHeaderV0) IsSupportedVersion(min, max int16) bool {
	return min <= r.ApiVersion &&
		r.ApiVersion <= max
}

func DeserializeRequest(b bytes.Buffer) *RequestHeaderV0 {
	r := RequestHeaderV0{}
	reader := bytes.NewReader(b.Bytes())
	binary.Read(reader, binary.BigEndian, &r)
	return &r
}
