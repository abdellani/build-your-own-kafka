package decoder

import (
	"encoding/binary"
	"os"

	"github.com/abdellani/build-your-own-kafka/app/types"
)

type Decoder struct {
	data   []byte
	offset int32
}

func (d *Decoder) GetRow(start, end int32) []byte {
	return d.data[start:end]
}

func (d *Decoder) SetOffset(offset int32) {
	d.offset = offset
}

func (d *Decoder) LoadBinaryFile(path string) {
	content, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	d.Init(content)

}
func (d *Decoder) Init(data []byte) {
	d.data = data

}
func (d *Decoder) GetInt64() (int64, error) {
	tmp := int64(binary.BigEndian.Uint64(d.data[d.offset:]))
	d.offset += 8
	return tmp, nil
}

func (d *Decoder) GetOffset() int32 {
	return d.offset
}
func (d *Decoder) GetInt32() (int32, error) {
	tmp := int32(binary.BigEndian.Uint32(d.data[d.offset:]))
	d.offset += 4
	return tmp, nil
}

func (d *Decoder) GetInt16() (int16, error) {
	tmp := int16(binary.BigEndian.Uint16(d.data[d.offset:]))
	d.offset += 2
	return tmp, nil
}

func (d *Decoder) GetInt8() (int8, error) {
	tmp := int8(d.data[d.offset])
	d.offset++
	return tmp, nil
}

func (d *Decoder) GetByte() (byte, error) {
	tmp := d.data[d.offset]
	d.offset++
	return tmp, nil
}

func (d *Decoder) GetBytes(n int32) ([]byte, error) {
	buffer := d.data[d.offset : d.offset+n]
	d.offset += n
	return buffer, nil
}

func (d *Decoder) GetVarint() (types.SIGNED_VARINT, error) {
	num, n := binary.Varint(d.data[d.offset:])
	d.offset += int32(n)
	return types.SIGNED_VARINT(num), nil
}

func (d *Decoder) GetUvarint() (types.UNSIGNED_VARINT, error) {
	num, n := binary.Uvarint(d.data[d.offset:])
	d.offset += int32(n)
	return types.UNSIGNED_VARINT(num), nil
}

func (d *Decoder) GetCompactString() (types.COMPACT_STRING, error) {
	l, _ := d.GetUvarint()
	//TODO handle the case when Uvarint is longer than 1 byte
	if l < 2 {
		return []byte{}, nil
	}

	return d.GetBytes(int32(l - 1))
}

func (d *Decoder) GetUUID() (types.UUID, error) {
	tmp := d.data[d.offset : d.offset+16]
	d.offset += 16
	return types.UUID(tmp), nil
}
func (d *Decoder) Advance(steps int32) {
	d.offset += steps
}

func (d Decoder) ReachedTheEnd() bool {
	return d.offset <= int32(len(d.data))-1
}

func (d Decoder) Remaining() int32 {
	return int32(len(d.data)) - int32(d.offset)
}

func (d *Decoder) GetCompactArrayInt32() (types.COMPACT_ARRAY[int32], error) {
	length, _ := d.GetUvarint()
	result := types.COMPACT_ARRAY[int32]{}
	if length < 1 {
		return result, nil
	}
	for i := int32(0); i < int32(length-1); i++ {
		number, _ := d.GetInt32()
		result = append(result, number)
	}
	return result, nil
}

func (d *Decoder) GetCompactArrayUUID() (types.COMPACT_ARRAY[types.UUID], error) {
	length, _ := d.GetUvarint()
	result := types.COMPACT_ARRAY[types.UUID]{}
	if length < 1 {
		return result, nil
	}
	for i := int32(0); i < int32(length-1); i++ {
		uuid, _ := d.GetUUID()
		result = append(result, uuid)
	}
	return result, nil
}
