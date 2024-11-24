package messages

type DTPHandler struct {
	SupportedVersions
}
type DTPResponse struct {
	ThrottleTimeMs int32
	NumTopics      int8
	Topics         []Topic
	NumNextCursor  int8
	NextCursors    []NextCursor
}

type Topic struct {
	ErrorCode                  int16
	LenName                    int8
	Name                       string
	TopicId                    string
	IsInternal                 bool
	NumPartitions              int8
	Partitions                 []Partition
	TopicsAuthorizedOperations int32
}

type UUID string
type NextCursor struct {
	NumTopicNames  int8
	TopicNames     []TopicName
	PartitionIndex int32
}

type TopicName string

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
	return 0
}

func (r *DTPResponse) Serialize() []byte {

	return nil
}
