package encoder

import (
	"bytes"
	"encoding/binary"

	"github.com/abdellani/build-your-own-kafka/app/types"
)

type Encoder struct {
	buffer bytes.Buffer
	offset int32
}

func (e Encoder) GetOffset() int32 {
	return e.offset
}

func (re *Encoder) PutInt32At(in int32, offset int, length int) {
	binary.BigEndian.PutUint32(re.buffer.Bytes()[offset:offset+length], uint32(in))
}

func (e *Encoder) Init() {
	e.buffer = bytes.Buffer{}
	e.offset = 0
}

func (e *Encoder) PutInt64(i int64) {
	binary.Write(&e.buffer, binary.BigEndian, i)
	e.offset += 8
}

func (e *Encoder) PutInt32(i int32) {
	binary.Write(&e.buffer, binary.BigEndian, i)
	e.offset += 4
}

func (e *Encoder) PutInt16(i int16) {
	binary.Write(&e.buffer, binary.BigEndian, i)
	e.offset += 2
}

func (e *Encoder) PutInt8(i int8) {
	binary.Write(&e.buffer, binary.BigEndian, i)
	e.offset += 1
}

func (e *Encoder) PutByte(i byte) {
	binary.Write(&e.buffer, binary.BigEndian, i)
	e.offset += 1
}

func (e *Encoder) PutVarint(i types.SIGNED_VARINT) {
	b := i.Serialize()
	binary.Write(&e.buffer, binary.BigEndian, b)
	e.offset += int32(len(b))
}
func (e *Encoder) PutUvarint(i types.UNSIGNED_VARINT) {
	b := i.Serialize()
	binary.Write(&e.buffer, binary.BigEndian, b)
	e.offset += int32(len(b))
}

func (e *Encoder) PutBytes(b []byte) {
	binary.Write(&e.buffer, binary.BigEndian, b)
	e.offset += int32(len(b))
}

func (e *Encoder) GetBytes() []byte {
	return e.buffer.Bytes()
}
