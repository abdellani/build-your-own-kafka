package messages

import "fmt"

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
	Name                       COMPACT_NULLABLE_STRING
	TopicId                    UUID
	IsInternal                 int8
	Partitions                 COMPACT_ARRAY[Partition]
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
	ReplicaNodes           COMPACT_ARRAY[int32]
	IsrNode                COMPACT_ARRAY[int32]
	EligibleLeaderReplicas COMPACT_ARRAY[int32]
	LastKnownElr           COMPACT_ARRAY[int32]
	OfflineReplicas        COMPACT_ARRAY[int32]
	TAG_BUFFER
}

const LogFilePath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

func (h *DTPHandler) Handle(r IRequest) IResponse {
	req := r.(*DTPRequest)
	res := h.NewReponse(req.CorrelationId)
	res.ProcessRequestedTopics(req.Topics)
	res.GenerateNuxtCursor()
	res.Size = res.CalculateSize()
	fmt.Printf("%+v\n", *res)
	return res
}

func (res *DTPResponse) GenerateNuxtCursor() {
	// for i := 0; i < len(res.Topics); i++ {
	// 	topic := res.Topics[i]
	// 	if topic.ErrorCode > 0 {
	// 		continue
	// 	}
	// 	for j := 0; j < len(topic.Partitions); j++ {
	// 		partition := topic.Partitions[j]
	// 		if partition.ErrorCode > 0 {
	// 			continue
	// 		}
	// 		res.NextCursor = NULLABLE_FIELD[NextCursor]{
	// 			IsNull: false,
	// 			Field: NextCursor{
	// 				TopicName:      COMPACT_STRING(topic.Name),
	// 				PartitionIndex: partition.PartitionIndex,
	// 			},
	// 		}
	// 		return
	// 	}
	// }
	res.NextCursor = NULLABLE_FIELD[NextCursor]{IsNull: true}
}

func (res *DTPResponse) ProcessRequestedTopics(requestedTopics COMPACT_ARRAY[DTPRequestTopic]) {
	topics := []Topic{}
	clusterMetada := LoadClusterMetaData(LogFilePath)

	for i := 0; i < len(requestedTopics); i++ {
		requestedTopic := requestedTopics[i]
		uuid, found := clusterMetada.GetTopicUUID(requestedTopic.Name)
		if !found {
			answer := Topic{
				ErrorCode: 3,
				Name:      COMPACT_NULLABLE_STRING(requestedTopic.Name),
				TopicId:   uuid,
			}
			topics = append(topics, answer)
			continue
		}
		partitionRecord, found := clusterMetada.GetPartitionRecords(uuid)
		if !found {
			answer := Topic{
				ErrorCode: 3,
				Name:      COMPACT_NULLABLE_STRING(requestedTopic.Name),
				TopicId:   uuid,
			}
			topics = append(topics, answer)
			continue
		}

		topic := Topic{
			ErrorCode:  0,
			Name:       COMPACT_NULLABLE_STRING(requestedTopic.Name),
			TopicId:    uuid,
			Partitions: COMPACT_ARRAY[Partition]{},
		}
		for j := 0; j < len(*partitionRecord); j++ {
			record := (*partitionRecord)[j]
			partition := Partition{
				ErrorCode:      0,
				PartitionIndex: record.PartitionID,
			}
			topic.Partitions = append(topic.Partitions, partition)

		}
		topics = append(topics, topic)
	}
	res.Topics = topics
}

func LoadClusterMetaData(path string) *ClusterMetadata {
	decoder := &Decoder{}
	cm := &ClusterMetadata{}
	decoder.LoadBinaryFile(path)
	cm.Init(decoder)
	cm.Decode()
	return cm
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
