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

func DecodeFetchRequestBody(decoder *Decoder) (*FetchRequestBody, bool) {
	req := &FetchRequestBody{}
	return req, false
}
