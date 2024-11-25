package messages

type DTPHandler struct {
	SupportedVersions
}
type DTPResponse struct {
	CommonResponseFields
	ResponseHeader
	ThrottleTimeMs int32
	NumTopics      int8
	Topics         []Topic
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
}

type UUID [16]byte
type NextCursor struct {
	NumTopicNames  int8
	TopicNames     String
	PartitionIndex int32
}

type String []byte

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

func (h *DTPHandler) Handle(req *Request) Response {

	return h.NewReponse(req.CorrelationId)
}

func (h *DTPHandler) NewReponse(correlationId int32) Response {
	return &DTPResponse{}
}

func (r *DTPResponse) CalculateSize() int32 {
	return CalculateSize(*r) - 4
}

func (r *DTPResponse) Serialize() []byte {

	return nil
}
