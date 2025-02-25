package cluster_metadata

import (
	"fmt"

	"github.com/abdellani/build-your-own-kafka/app/decoder"
	"github.com/abdellani/build-your-own-kafka/app/encoder"
)

func (v *Value) isTopicRecord() bool {
	return v.Type == VALUE_TYPE_TOPIC_RECORD
}

func (v *Value) isPartitionRecord() bool {
	return v.Type == VALUE_TYPE_PARTITION_RECORD
}
func DecodeValue(decoder *decoder.Decoder, valueLength int64) *Value {
	value := &Value{}
	start := decoder.GetOffset()

	value.FrameVersion, _ = decoder.GetInt8()
	value.Type, _ = decoder.GetInt8()
	value.Version, _ = decoder.GetInt8()
	switch value.Type {
	case VALUE_TYPE_TOPIC_RECORD:
		value.Details = *DecodeTopicRecordDetails(decoder)
	case VALUE_TYPE_PARTITION_RECORD:
		value.Details = *DecodePartitionRecordDetails(decoder)
	default:
		decoder.Advance(int32(valueLength) - 3)
	}

	end := decoder.GetOffset()

	if int32(valueLength) != end-start {
		err := fmt.Sprintf(
			"Error: value not parsed properly.\n length %d, parsed %d\n",
			valueLength,
			end-start)
		panic(err)
	}
	return value
}
func (v *Value) Encode(e *encoder.Encoder) {
	e.PutInt8(v.FrameVersion)
	e.PutInt8(v.Type)
	e.PutInt8(v.Version)
	var details ValueDetails
	switch v.Type {
	case VALUE_TYPE_TOPIC_RECORD:
		details = v.Details.(ValueTopicsRecord)
	case VALUE_TYPE_PARTITION_RECORD:
		details = v.Details.(ValuePartitionRecord)
	default:
		panic("unknown case")
	}
	details.Encode(e)
}
