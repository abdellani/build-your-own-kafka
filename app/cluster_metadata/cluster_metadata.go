package cluster_metadata

import (
	"reflect"

	"github.com/abdellani/build-your-own-kafka/app/decoder"

	"github.com/abdellani/build-your-own-kafka/app/types"
)

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

func (c *ClusterMetadata) Init(d *decoder.Decoder) {
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

func DecodeTopicRecordDetails(decoder *decoder.Decoder) *ValueTopicsRecord {
	value := &ValueTopicsRecord{}
	value.TopicName, _ = decoder.GetCompactString()
	value.TopicUUID, _ = decoder.GetUUID()
	tag, _ := decoder.GetUvarint()
	value.TAG_BUFFER = types.TAG_BUFFER(tag)
	return value
}

func DecodePartitionRecordDetails(decoder *decoder.Decoder) *ValuePartitionRecord {
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
	value.TAG_BUFFER = types.TAG_BUFFER(tag)
	return value
}

func (cm *ClusterMetadata) GetTopicUUID(topicName types.COMPACT_STRING) (types.UUID, bool) {
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
	return types.UUID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, false
}

func (cm *ClusterMetadata) GetPartitionRecords(uuid types.UUID) (*[]ValuePartitionRecord, bool) {
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

func (cm *ClusterMetadata) FindTopicName(uuid types.UUID) string {
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

func LoadClusterMetaData(path string) *ClusterMetadata {
	decoder := &decoder.Decoder{}
	cm := &ClusterMetadata{}
	decoder.LoadBinaryFile(path)
	cm.Init(decoder)
	cm.Decode()
	return cm
}
