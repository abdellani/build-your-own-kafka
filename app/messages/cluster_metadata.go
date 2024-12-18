package messages

import (
	"fmt"
	"reflect"
)

type ClusterMetadata struct {
	RecordBatchs COMPACT_ARRAY[RecordBatch]
	decoder      *Decoder
}
type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
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
	Records              ARRAY[Record]
}

type Record struct {
	Length           SIGNED_VARINT
	Attributes       int8
	TimestampDelta   SIGNED_VARINT
	OffsetDelta      SIGNED_VARINT
	KeyLength        SIGNED_VARINT
	Key              []byte
	ValueLength      SIGNED_VARINT
	Value            Value
	HeaderArrayCount UNSIGNED_VARINT
}

func (r *Record) Encode(e *Encoder) {
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

const VALUE_TYPE_TOPIC_RECORD = 2
const VALUE_TYPE_PARTITION_RECORD = 3

type Value struct {
	FrameVersion int8
	Type         int8
	Version      int8
	Details      ValueDetails
}

func (v *Value) Encode(e *Encoder) {
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

type ValueDetails interface {
	Encode(e *Encoder)
}

type ValueTopicsRecord struct {
	TopicName COMPACT_STRING
	TopicUUID UUID
	TAG_BUFFER
}

func (v ValueTopicsRecord) Encode(e *Encoder) {
	e.PutBytes(v.TopicName.Serialize())
	e.PutBytes(v.TopicUUID[:])
	e.PutVarint(0)
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

func (v ValuePartitionRecord) Encode(e *Encoder) {
	e.PutInt32(v.PartitionID)
	e.PutBytes(v.TopicUUID[:])
	e.PutBytes(v.ReplicaArray.Serialize())
	e.PutBytes(v.InSyncReplicaArray.Serialize())
	e.PutBytes(v.RemovingReplicaArray.Serialize())
	e.PutBytes(v.AddingReplicaArray.Serialize())
	e.PutInt32(v.Leader)
	e.PutInt32(v.LeaderEpoch)
	e.PutInt32(v.PartitionEpoch)
	e.PutBytes(v.DirectoriesArray.Serialize())
	e.PutVarint(SIGNED_VARINT(v.TAG_BUFFER))
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
func (c *ClusterMetadata) GetRow(start, end int32) []byte {
	return c.decoder.GetRow(start, end)
}

func (c *ClusterMetadata) Init(d *Decoder) {
	c.decoder = d
}
func (c *ClusterMetadata) Decode() {
	var records []RecordBatch
	d := c.decoder
	for d.ReachedTheEnd() {
		records = append(records, *DecodeRecordBatch(d))
	}
	c.RecordBatchs = records
}
func (r *RecordBatch) Encode() []byte {
	encoder := Encoder{}
	encoder.PutInt64(0)
	encoder.PutInt32(r.BatchLength)
	encoder.PutInt32(r.PartitionLeaderEpoch)
	encoder.PutByte(r.MagicByte)
	crcIndex := encoder.GetOffset()
	encoder.PutInt32(r.CRC)
	encoder.PutInt16(r.Attributes)
	encoder.PutInt32(r.LastOffsetDelta)
	encoder.PutInt64(r.BaseTimestamp)
	encoder.PutInt64(r.MaxTimestamp)
	encoder.PutInt64(r.ProducerID)
	encoder.PutInt16(r.ProducerEpoch)
	encoder.PutInt32(r.BaseSequence)
	encoder.PutInt32(1)
	r.Records[0].Encode(&encoder)
	// currentIndex := encoder.GetOffset()
	// crcData := encoder.GetBytes()[crcIndex+4 : currentIndex]
	//checksum := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	checksum := 0
	encoder.PutInt32At(int32(checksum), int(crcIndex), 4)
	return encoder.GetBytes()
}
func DecodeRecordBatch(decoder *Decoder) *RecordBatch {
	batchRecord := RecordBatch{}
	batchRecord.BaseOffset, _ = decoder.GetInt64()
	//Batch Length: 4 bytes
	batchRecord.BatchLength, _ = decoder.GetInt32()

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
	record.HeaderArrayCount = UNSIGNED_VARINT(headerArrayCount)
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

func DecodeValue(decoder *Decoder, valueLength int64) *Value {
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

func (cm *ClusterMetadata) GetPartitionRecords(uuid UUID) (*[]ValuePartitionRecord, bool) {
	values := cm.GetValues()
	records := []ValuePartitionRecord{}
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
		records = append(records, partitionRecord)
	}
	return &records, len(records) > 0
}

func (cm *ClusterMetadata) FindTopicName(uuid UUID) string {
	batches := cm.RecordBatchs
	for i := 0; i < len(batches); i++ {
		batch := batches[i]
		values := batch.GetValues()
		for j := 0; j < len(values); j++ {
			value := values[j]
			if !value.isTopicRecord() {
				continue
			}
			v, _ := value.Details.(ValueTopicsRecord)
			if !reflect.DeepEqual(v.TopicUUID, uuid) {
				continue
			}
			return string(v.TopicName)
		}
	}
	return ""
}
