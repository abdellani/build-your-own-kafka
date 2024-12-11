package messages

type DTPHandler struct {
	SupportedVersions
}
type DTPResponse struct {
	Size int32
	ResponseHeaderV1
	ThrottleTimeMs int32
	Topics         COMPACT_ARRAY[Topic]
	NextCursor     NULLABLE_FIELD[NextCursor]
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
	TopicName      COMPACT_STRING
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
	TAG_BUFFER
}

func (h *DTPHandler) Handle(r IRequest) IResponse {

	req := r.(*DTPRequest)
	//r.DTPRequestBody.Topics.Items[0]
	res := h.NewReponse(req.CorrelationId)
	topic := Topic{
		ErrorCode: 3,
		LenName:   int8(len(req.Topics[0].Name)) + 1,
		Name:      req.Topics[0].Name,
		TopicId:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	res.Topics = append(res.Topics, topic)
	res.NextCursor = NULLABLE_FIELD[NextCursor]{IsNull: true}
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
