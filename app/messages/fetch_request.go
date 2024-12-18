package messages

type FetchRequest struct {
	Size int32
	RequestHeaderV2
	FetchRequestBody
}

type FetchRequestBody struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionID           int32
	SessionEpoch        int32
	Topics              COMPACT_ARRAY[FetchRequestTopic]
	ForgottenTopicsData COMPACT_ARRAY[FetchRequestForgottenTopicData]
	RackId              COMPACT_STRING
	TAG_BUFFER
}
type FetchRequestTopic struct {
	TopicID    UUID
	Partitions COMPACT_ARRAY[FetchRequestPartition]
	TAG_BUFFER
}

type FetchRequestPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
	TAG_BUFFER
}

type FetchRequestForgottenTopicData struct {
	TopicID    UUID
	Partitions COMPACT_ARRAY[int32]
	TAG_BUFFER
}

func DecodeFetchRequestBody(d *Decoder) (*FetchRequestBody, bool) {
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
func DecodeFetchRequestTopics(d *Decoder) (*COMPACT_ARRAY[FetchRequestTopic], error) {
	length, _ := d.GetUvarint()
	result := COMPACT_ARRAY[FetchRequestTopic]{}
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
func DecodeFetchRequestPartition(d *Decoder) (*COMPACT_ARRAY[FetchRequestPartition], error) {
	partitions := COMPACT_ARRAY[FetchRequestPartition]{}
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
		partition.TAG_BUFFER = TAG_BUFFER(tag)
		partitions = append(partitions, partition)
	}
	return &partitions, nil
}
