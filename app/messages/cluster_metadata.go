package messages

import (
	"fmt"
	"reflect"
)

type ClusterMetadata struct {
	RecordBatchs []RecordBatch
	decoder      *Decoder
}
type RecordBatch struct {
	BaseOffset           int64
	PartitionLeaderEpoch int32
	MagicByte            byte
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64 // -1 means ID not set
	ProducerEpoch        int16 // -1 mean non applicable
	BaseSequence         int32 // -1 means not set
	Records              []Record
}

type Record struct {
	Attributes       int8
	TimestampDelta   SIGNED_VARINT
	OffsetDelfa      SIGNED_VARINT
	Key              []byte
	Value            Value
	HeaderArrayCount UNSIGNED_VARINT
}

const VALUE_TYPE_TOPIC_RECORD = 2
const VALUE_TYPE_PARTITION_RECORD = 3

type Value struct {
	FrameVersion int8
	Type         int8
	Version      int8
	Details      ValueDetails
}

type ValueDetails interface {
}

type ValueTopicsRecord struct {
	TopicName COMPACT_STRING
	TopicUUID UUID
	TAG_BUFFER
}

type ValuePartitionRecord struct {
	PartitionID          int32
	TopicUUID            UUID
	ReplicaArray         COMPACT_ARRAY[int32]
	InSyncReplicaArray   COMPACT_ARRAY[int32]
	RemovingReplicaArray COMPACT_ARRAY[int32]
	AddingReplicaArray   COMPACT_ARRAY[int32]
	Leader               int32
	LeaderEpoch          int32
	PartitionEpoch       int32
	DirectoriesArray     COMPACT_ARRAY[UUID]
	TAG_BUFFER
}

func (r *RecordBatch) GetValues() []Value {
	result := []Value{}
	for i := 0; i < len(r.Records); i++ {
		record := r.Records[i]
		result = append(result, record.Value)
	}
	return result
}
func (c *ClusterMetadata) GetValues() []Value {
	result := []Value{}
	for i := 0; i < len(c.RecordBatchs); i++ {
		record := c.RecordBatchs[i]
		result = append(result, record.GetValues()...)
	}
	return result
}

func (c *ClusterMetadata) Init(d *Decoder) {
	c.decoder = d
}
func (c *ClusterMetadata) Decoode() {
	var records []RecordBatch
	d := c.decoder
	for d.ReachedTheEnd() {
		records = append(records, *DecodeRecordBatch(d))
	}
	c.RecordBatchs = records
}

func DecodeRecordBatch(decoder *Decoder) *RecordBatch {
	batchRecord := RecordBatch{}
	batchRecord.BaseOffset, _ = decoder.GetInt64()
	//Batch Length: 4 bytes
	decoder.GetInt32()

	batchRecord.PartitionLeaderEpoch, _ = decoder.GetInt32()
	batchRecord.MagicByte, _ = decoder.GetByte()
	batchRecord.CRC, _ = decoder.GetInt32()
	batchRecord.Attributes, _ = decoder.GetInt16()
	batchRecord.LastOffsetDelta, _ = decoder.GetInt32()
	batchRecord.BaseTimestamp, _ = decoder.GetInt64()
	batchRecord.MaxTimestamp, _ = decoder.GetInt64()
	batchRecord.ProducerID, _ = decoder.GetInt64()
	batchRecord.ProducerEpoch, _ = decoder.GetInt16()
	batchRecord.BaseSequence, _ = decoder.GetInt32()

	recordsNum, _ := decoder.GetInt32()

	for i := int32(0); i < recordsNum; i++ {
		batchRecord.Records = append(batchRecord.Records, *DecordRecord(decoder))
	}

	return &batchRecord
}

func DecordRecord(decoder *Decoder) *Record {
	record := &Record{}
	length, _ := decoder.GetVarint() // Record length
	start := decoder.GetOffset()

	record.Attributes, _ = decoder.GetInt8()
	record.TimestampDelta, _ = decoder.GetVarint()
	record.OffsetDelfa, _ = decoder.GetVarint()
	keyLength, _ := decoder.GetVarint()
	if keyLength > 0 {
		record.Key, _ = decoder.GetBytes(int32(keyLength))
	}
	record.Value = *DecodeValue(decoder)
	record.HeaderArrayCount, _ = decoder.GetUvarint()

	end := decoder.GetOffset()
	if int32(length) != (end - start) {
		message := fmt.Sprintf(
			"Error: Record not parsed properly,message length %d, parsed %d",
			length,
			end-start)
		panic(message)
	}
	return record
}

func DecodeValue(decoder *Decoder) *Value {
	valueLength, _ := decoder.GetVarint() // value length
	if valueLength <= 0 {
		return &Value{}
	}
	start := decoder.GetOffset()

	value := &Value{}
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

func DecodeTopicRecordDetails(decoder *Decoder) *ValueTopicsRecord {
	value := &ValueTopicsRecord{}
	value.TopicName, _ = decoder.GetCompactString()
	value.TopicUUID, _ = decoder.GetUUID()
	tag, _ := decoder.GetUvarint()
	value.TAG_BUFFER = TAG_BUFFER(tag)
	return value
}

func DecodePartitionRecordDetails(decoder *Decoder) *ValuePartitionRecord {
	value := &ValuePartitionRecord{}
	value.PartitionID, _ = decoder.GetInt32()
	value.TopicUUID, _ = decoder.GetUUID()
	value.ReplicaArray, _ = decoder.GetCompactArrayInt32()
	value.InSyncReplicaArray, _ = decoder.GetCompactArrayInt32()
	value.RemovingReplicaArray, _ = decoder.GetCompactArrayInt32()
	value.AddingReplicaArray, _ = decoder.GetCompactArrayInt32()
	value.Leader, _ = decoder.GetInt32()
	value.LeaderEpoch, _ = decoder.GetInt32()
	value.PartitionEpoch, _ = decoder.GetInt32()
	value.DirectoriesArray, _ = decoder.GetCompactArrayUUID()
	tag, _ := decoder.GetUvarint()
	value.TAG_BUFFER = TAG_BUFFER(tag)
	return value
}

func (v *Value) isTopicRecord() bool {
	return v.Type == VALUE_TYPE_TOPIC_RECORD
}

func (v *Value) isPartitionRecord() bool {
	return v.Type == VALUE_TYPE_PARTITION_RECORD
}

func (cm *ClusterMetadata) GetTopicUUID(topicName COMPACT_STRING) (UUID, bool) {
	values := cm.GetValues()
	for i := 0; i < len(values); i++ {
		value := values[i]
		if !value.isTopicRecord() {
			continue
		}
		topic, ok := value.Details.(ValueTopicsRecord)
		if !ok {
			panic("Error while trying to load metadata")
		}
		if !reflect.DeepEqual(topicName, topic.TopicName) {
			continue
		}
		return topic.TopicUUID, true
	}
	return UUID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, false
}

func (cm *ClusterMetadata) GetPartitionRecord(uuid UUID) (*ValuePartitionRecord, bool) {
	values := cm.GetValues()
	for i := 0; i < len(values); i++ {
		value := values[i]
		if !value.isPartitionRecord() {
			continue
		}
		partitionRecord, ok := value.Details.(ValuePartitionRecord)
		if !ok {
			panic("Error while trying to load metadata")
		}
		if !reflect.DeepEqual(uuid, partitionRecord.TopicUUID) {
			continue
		}
		return &partitionRecord, true
	}
	return nil, false
}
