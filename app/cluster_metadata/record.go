package cluster_metadata

import (
	"fmt"

	"github.com/abdellani/build-your-own-kafka/app/decoder"
	"github.com/abdellani/build-your-own-kafka/app/encoder"
	"github.com/abdellani/build-your-own-kafka/app/types"
)

type Record struct {
	Length           types.SIGNED_VARINT
	Attributes       int8
	TimestampDelta   types.SIGNED_VARINT
	OffsetDelta      types.SIGNED_VARINT
	KeyLength        types.SIGNED_VARINT
	Key              []byte
	ValueLength      types.SIGNED_VARINT
	Value            Value
	HeaderArrayCount types.UNSIGNED_VARINT
}

func (r *Record) Encode(e *encoder.Encoder) {
	e.PutVarint(r.Length)
	e.PutInt8(r.Attributes)
	e.PutVarint(r.TimestampDelta)
	e.PutVarint(r.OffsetDelta)
	e.PutVarint(r.KeyLength)
	if r.KeyLength > 0 {
		e.PutBytes(r.Key)
	}
	e.PutVarint(r.ValueLength)
	if r.ValueLength > 0 {
		r.Value.Encode(e)
	}
	e.PutUvarint(r.HeaderArrayCount)
}

func DecordRecord(decoder *decoder.Decoder) *Record {
	record := &Record{}
	record.Length, _ = decoder.GetVarint() // Record length
	start := decoder.GetOffset()

	record.Attributes, _ = decoder.GetInt8()
	record.TimestampDelta, _ = decoder.GetVarint()
	record.OffsetDelta, _ = decoder.GetVarint()
	record.KeyLength, _ = decoder.GetVarint()
	if record.KeyLength > 0 {
		record.Key, _ = decoder.GetBytes(int32(record.KeyLength))
	}
	record.ValueLength, _ = decoder.GetVarint()
	if record.ValueLength > 0 {
		record.Value = *DecodeValue(decoder, int64(record.ValueLength))
	}
	headerArrayCount, _ := decoder.GetUvarint()
	record.HeaderArrayCount = types.UNSIGNED_VARINT(headerArrayCount)
	end := decoder.GetOffset()
	if int32(record.Length) != (end - start) {
		message := fmt.Sprintf(
			"Error: Record not parsed properly,message length %d, parsed %d",
			record.Length,
			end-start)
		panic(message)
	}
	return record
}
