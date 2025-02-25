package dtp

import (
	"github.com/abdellani/build-your-own-kafka/app/cluster_metadata"
	"github.com/abdellani/build-your-own-kafka/app/constants"
	"github.com/abdellani/build-your-own-kafka/app/messages"
	"github.com/abdellani/build-your-own-kafka/app/types"
	"github.com/abdellani/build-your-own-kafka/app/utils"
)

type DTPHandler struct {
	messages.SupportedVersions
}
type DTPResponse struct {
	Size int32
	types.ResponseHeaderV1
	ThrottleTimeMs int32
	Topics         types.COMPACT_ARRAY[Topic]
	NextCursor     types.NULLABLE_FIELD[NextCursor]
	types.TAG_BUFFER
}

type Topic struct {
	ErrorCode                  int16
	Name                       types.COMPACT_NULLABLE_STRING
	TopicId                    types.UUID
	IsInternal                 int8
	Partitions                 types.COMPACT_ARRAY[Partition]
	TopicsAuthorizedOperations int32
	types.TAG_BUFFER
}

type NextCursor struct {
	TopicName      types.COMPACT_STRING
	PartitionIndex int32
	types.TAG_BUFFER
}

type Partition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           types.COMPACT_ARRAY[int32]
	IsrNode                types.COMPACT_ARRAY[int32]
	EligibleLeaderReplicas types.COMPACT_ARRAY[int32]
	LastKnownElr           types.COMPACT_ARRAY[int32]
	OfflineReplicas        types.COMPACT_ARRAY[int32]
	types.TAG_BUFFER
}

func (h *DTPHandler) Handle(r types.IRequest) types.IResponse {
	req := r.(*DTPRequest)
	res := h.NewReponse(req.CorrelationId)
	res.ProcessRequestedTopics(req.Topics)
	res.GenerateNuxtCursor()
	res.Size = res.CalculateSize()
	return res
}

func (res *DTPResponse) GenerateNuxtCursor() {
	res.NextCursor = types.NULLABLE_FIELD[NextCursor]{IsNull: true}
}

func (res *DTPResponse) ProcessRequestedTopics(requestedTopics types.COMPACT_ARRAY[DTPRequestTopic]) {
	topics := []Topic{}
	clusterMetada := cluster_metadata.LoadClusterMetaData(constants.LogFilePath)

	for i := 0; i < len(requestedTopics); i++ {
		requestedTopic := requestedTopics[i]
		uuid, found := clusterMetada.GetTopicUUID(requestedTopic.Name)
		if !found {
			answer := Topic{
				ErrorCode: 3,
				Name:      types.COMPACT_NULLABLE_STRING(requestedTopic.Name),
				TopicId:   uuid,
			}
			topics = append(topics, answer)
			continue
		}
		partitionRecord, found := clusterMetada.GetPartitionRecords(uuid)
		if !found {
			answer := Topic{
				ErrorCode: 3,
				Name:      types.COMPACT_NULLABLE_STRING(requestedTopic.Name),
				TopicId:   uuid,
			}
			topics = append(topics, answer)
			continue
		}

		topic := Topic{
			ErrorCode:  0,
			Name:       types.COMPACT_NULLABLE_STRING(requestedTopic.Name),
			TopicId:    uuid,
			Partitions: types.COMPACT_ARRAY[Partition]{},
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

func (h *DTPHandler) NewReponse(correlationId int32) *DTPResponse {
	r := &DTPResponse{}
	r.CorrelationID = correlationId
	return r
}

func (r *DTPResponse) CalculateSize() int32 {
	return utils.CalculateSize(*r) - 4
}

func (r DTPResponse) Serialize() []byte {
	return types.Serialize(r)
}
