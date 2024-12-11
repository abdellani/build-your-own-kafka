package messages

type DTPHandler struct {
	SupportedVersions
}
type DTPResponse struct {
	Size int32
	ResponseHeaderV1
	ThrottleTimeMs int32
	NumTopics      int8
	Topics         []Topic //COMPACT_ARRAY[Topic]
	NextCursor     NextCursor
	TAG_BUFFER
}

type Topic struct {
	ErrorCode                  int16
	LenName                    int8
	Name                       []byte
	TopicId                    UUID
	IsInternal                 int8
	NumPartitions              int8
	Partitions                 []Partition
	TopicsAuthorizedOperations int32
	TAG_BUFFER
}

type NextCursor struct {
	NumTopicNames  int8
	TopicNames     String
	PartitionIndex int32
	TAG_BUFFER
}

type Partition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           int32
	IsrNode                int32
	EligibleLeaderReplicas int32
	LastKnownElr           int32
	OfflineReplicas        int32
}

func (h *DTPHandler) Handle(r IRequest) IResponse {

	req := r.(*DTPRequest)
	//r.DTPRequestBody.Topics.Items[0]
	res := h.NewReponse(req.CorrelationId)
	res.NumTopics = 2
	topic := Topic{
		ErrorCode: 3,
		LenName:   int8(len(req.Topics[0].Name.String)) + 1,
		Name:      req.Topics[0].Name.String,
		TopicId:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	res.Topics = append(res.Topics, topic)
	res.NextCursor = NextCursor{
		NumTopicNames: -1,
	}
	res.Size = res.CalculateSize()
	return res
}

func (h *DTPHandler) NewReponse(correlationId int32) *DTPResponse {
	r := &DTPResponse{}
	r.CorrelationID = correlationId
	return r
}

func (r *DTPResponse) CalculateSize() int32 {
	return CalculateSize(*r) - 4
}

func (r DTPResponse) Serialize() []byte {
	return Serialize(r)
}
