package messages

type ClusterMetadata struct {
	RecordBatchs []RecordBatch
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
	Length           UNSIGNED_VARINT
	Attributes       byte
	TimestampDelta   SIGNED_VARINT
	OffsetDelfa      SIGNED_VARINT
	Key              []byte
	Value            Value
	HeaderArrayCount UNSIGNED_VARINT
}

type Value struct {
	FrameVersion byte
	Type         byte
	Version      byte
	Name         []byte
	FeatureLevel int16
	TAG_BUFFER
}

func DecodeClusterMetada(d *Decoder) *ClusterMetadata {

	recordBatch := DecodeRecordBatch(d)

	return &ClusterMetadata{
		[]RecordBatch{
			*recordBatch,
		},
	}
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
		record := Record{}

		batchRecord.Records = append(batchRecord.Records, record)
	}

	return &batchRecord
}

func DecordRecord(decoder *Decoder) *Record {
	record := &Record{}
	//record.
	return record
}
