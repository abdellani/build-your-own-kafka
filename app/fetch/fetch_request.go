package fetch

import (
	"github.com/abdellani/build-your-own-kafka/app/decoder"
	"github.com/abdellani/build-your-own-kafka/app/types"
)

type FetchRequest struct {
	Size int32
	types.RequestHeaderV2
	FetchRequestBody
}

type FetchRequestBody struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionID           int32
	SessionEpoch        int32
	Topics              types.COMPACT_ARRAY[FetchRequestTopic]
	ForgottenTopicsData types.COMPACT_ARRAY[FetchRequestForgottenTopicData]
	RackId              types.COMPACT_STRING
	types.TAG_BUFFER
}
type FetchRequestTopic struct {
	TopicID    types.UUID
	Partitions types.COMPACT_ARRAY[FetchRequestPartition]
	types.TAG_BUFFER
}

type FetchRequestPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
	types.TAG_BUFFER
}

type FetchRequestForgottenTopicData struct {
	TopicID    types.UUID
	Partitions types.COMPACT_ARRAY[int32]
	types.TAG_BUFFER
}

func DecodeFetchRequestBody(d *decoder.Decoder) (*FetchRequestBody, bool) {
	req := &FetchRequestBody{}
	req.MaxWaitMs, _ = d.GetInt32()
	req.MinBytes, _ = d.GetInt32()
	req.MaxBytes, _ = d.GetInt32()
	req.IsolationLevel, _ = d.GetInt8()
	req.SessionID, _ = d.GetInt32()
	req.SessionEpoch, _ = d.GetInt32()
	topics, _ := DecodeFetchRequestTopics(d)
	req.Topics = *topics
	return req, false
}
func DecodeFetchRequestTopics(d *decoder.Decoder) (*types.COMPACT_ARRAY[FetchRequestTopic], error) {
	length, _ := d.GetUvarint()
	result := types.COMPACT_ARRAY[FetchRequestTopic]{}
	if length < 2 {
		return &result, nil
	}
	for i := 1; i < int(length); i++ {
		topic := FetchRequestTopic{}
		topic.TopicID, _ = d.GetUUID()
		partition, _ := DecodeFetchRequestPartition(d)
		topic.Partitions = *partition
		d.GetUvarint()
		result = append(result, topic)
	}
	return &result, nil
}
func DecodeFetchRequestPartition(d *decoder.Decoder) (*types.COMPACT_ARRAY[FetchRequestPartition], error) {
	partitions := types.COMPACT_ARRAY[FetchRequestPartition]{}
	length, _ := d.GetUvarint()
	for i := 1; i < int(length); i++ {
		partition := FetchRequestPartition{}
		partition.Partition, _ = d.GetInt32()
		partition.CurrentLeaderEpoch, _ = d.GetInt32()
		partition.FetchOffset, _ = d.GetInt64()
		partition.LastFetchedEpoch, _ = d.GetInt32()
		partition.LogStartOffset, _ = d.GetInt64()
		partition.PartitionMaxBytes, _ = d.GetInt32()
		tag, _ := d.GetInt8()
		partition.TAG_BUFFER = types.TAG_BUFFER(tag)
		partitions = append(partitions, partition)
	}
	return &partitions, nil
}
