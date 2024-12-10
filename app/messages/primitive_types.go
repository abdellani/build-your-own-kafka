package messages

import (
	"encoding/binary"
)

type UUID [16]byte

type String struct {
	Length  []byte
	Content []byte
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
type COMPACT_ARRAY[T any] struct {
	N     []byte // item count + 1
	Items []T
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

type RequestHeaderV2 struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      NULLABLE_STRING
	TAG_BUFFER
}

func (r RequestHeaderV2) ApiKeyValue() int16 {
	return r.ApiKey
}

type ResponseHeaderV0 struct {
	CorrelationID int32
}
type ResponseHeaderV1 struct {
	CorrelationID int32
	TAG_BUFFER
}

func DeserializeRequestHeaderV2(bytes []byte, offset int) (*RequestHeaderV2, int) {
	req := &RequestHeaderV2{}
	buffer := bytes[offset : offset+2]
	offset += 2
	req.ApiKey = int16(binary.BigEndian.Uint16(buffer))

	buffer = bytes[offset : offset+2]
	offset += 2
	req.ApiVersion = int16(binary.BigEndian.Uint16(buffer))

	buffer = bytes[offset : offset+4]
	offset += 4
	req.CorrelationId = int32(binary.BigEndian.Uint32(buffer))

	buffer = bytes[offset : offset+2]
	offset += 2
	req.ClientId.N = int16(binary.BigEndian.Uint16(buffer))

	buffer = bytes[offset : offset+int(req.ClientId.N)]
	offset += int(req.ClientId.N)
	req.ClientId.String = buffer

	buffer = bytes[offset : offset+1]
	req.TAG_BUFFER = TAG_BUFFER(buffer[0])
	offset += 1

	return req, offset
}
