package cluster_metadata

import (
	"github.com/abdellani/build-your-own-kafka/app/decoder"
	"github.com/abdellani/build-your-own-kafka/app/encoder"
	"github.com/abdellani/build-your-own-kafka/app/types"
)

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
	Records              types.ARRAY[Record]
}

func (r *RecordBatch) GetValues() []Value {
	result := []Value{}
	for i := 0; i < len(r.Records); i++ {
		record := r.Records[i]
		result = append(result, record.Value)
	}
	return result
}

func (r *RecordBatch) Encode() []byte {
	encoder := encoder.Encoder{}
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
func DecodeRecordBatch(decoder *decoder.Decoder) *RecordBatch {
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
