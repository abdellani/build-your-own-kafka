package messages

import (
	"bytes"
	"encoding/binary"
)

type UUID [16]byte
type ARRAY[T any] []T
type COMPACT_ARRAY[T any] []T
type COMPACT_STRING []byte
type NULLABLE_STRING []byte
type COMPACT_NULLABLE_STRING []byte
type UNSIGNED_VARINT = uint64
type SIGNED_VARINT = int64
type NULLABLE_FIELD[T any] struct {
	IsNull bool
	Field  T
}

func DeserializeNullableString(bytes []byte, offset int) (*NULLABLE_STRING, int) {
	s := NULLABLE_STRING{}
	buffer := bytes[offset : offset+2]
	offset += 2
	n := int16(binary.BigEndian.Uint16(buffer))
	if n == -1 {
		return &s, offset
	}

	s = bytes[offset : offset+int(n)]
	offset += int(n)

	return &s, offset
}

func (s COMPACT_NULLABLE_STRING) Serialize() []byte {
	buffer := bytes.Buffer{}
	//TODO use varint
	n := byte(len(s) + 1)
	binary.Write(&buffer, binary.BigEndian, n)
	if len(s) > 0 {
		binary.Write(&buffer, binary.BigEndian, s)
	}
	return buffer.Bytes()

}
func (s COMPACT_NULLABLE_STRING) IsPrimitiveType() bool { return true }
func (s NULLABLE_STRING) Serialize() []byte {
	buffer := bytes.Buffer{}
	n := int16(len(s))
	if n == 0 {
		binary.Write(&buffer, binary.BigEndian, int16(-1))
		return buffer.Bytes()
	}
	binary.Write(&buffer, binary.BigEndian, n)
	binary.Write(&buffer, binary.BigEndian, s)
	return buffer.Bytes()

}

func (s NULLABLE_STRING) IsPrimitiveType() bool { return true }

func (s COMPACT_STRING) Serialize() []byte {
	buffer := bytes.Buffer{}
	//TODO use varint
	n := byte(len(s) + 1)
	binary.Write(&buffer, binary.BigEndian, n)
	binary.Write(&buffer, binary.BigEndian, s)
	return buffer.Bytes()
}
func (s COMPACT_STRING) IsPrimitiveType() bool { return true }

func (f NULLABLE_FIELD[T]) Serialize() []byte {
	if f.IsNull {
		return []byte{0xFF}
	}
	return Serialize(f.Field)
}

func (NULLABLE_FIELD[T]) IsPrimitiveType() bool { return true }
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

func (C COMPACT_ARRAY[T]) IsPrimitiveType() bool { return true }

type ISerializable interface {
	Serialize() []byte
}
type IPrimitiveType interface {
	IsPrimitiveType() bool
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
