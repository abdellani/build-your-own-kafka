package messages

import (
	"bytes"
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

func DeserializeNullableString(bytes []byte, offset int) (*NULLABLE_STRING, int) {
	s := &NULLABLE_STRING{}
	buffer := bytes[offset : offset+2]
	offset += 2
	s.N = int16(binary.BigEndian.Uint16(buffer))
	if s.N == -1 {
		return s, offset
	}

	s.String = bytes[offset : offset+int(s.N)]
	offset += int(s.N)

	return s, offset
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
type COMPACT_ARRAY[T any] []T

func (c COMPACT_ARRAY[T]) Serialize() []byte {
	buffer := bytes.Buffer{}
	//TODO: use varint
	n := byte(len(c) + 1)
	binary.Write(&buffer, binary.BigEndian, n)
	if len(c) == 0 {
		return buffer.Bytes()
	}
	for i := 0; i < len(c); i++ {
		item := c[i]
		binary.Write(&buffer, binary.BigEndian, Serialize(item))
	}
	return buffer.Bytes()
}

func (C COMPACT_ARRAY[T]) isPrimitiveType() bool { return true }

type ISerializable interface {
	Serialize() []byte
}
type IPrimitiveType interface {
	isPrimitiveType() bool
}

type RequestHeaderV0 struct {
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

	clientId, offset := DeserializeNullableString(bytes, offset)
	req.ClientId = *clientId

	buffer = bytes[offset : offset+1]
	req.TAG_BUFFER = TAG_BUFFER(buffer[0])
	offset += 1

	return req, offset
}
