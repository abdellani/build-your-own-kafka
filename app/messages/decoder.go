package messages

import "encoding/binary"

type Decoder struct {
	data   []byte
	offset int32
}

func (d *Decoder) Init(data []byte) {
	d.data = data

}
func (d *Decoder) GetInt64() (int64, error) {
	tmp := int64(binary.BigEndian.Uint64(d.data[d.offset:]))
	d.offset += 8
	return tmp, nil
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
	return buffer, nil
}
