package fetch

import (
	"fmt"
	"os"
	"strings"

	"github.com/abdellani/build-your-own-kafka/app/cluster_metadata"
	"github.com/abdellani/build-your-own-kafka/app/constants"
	"github.com/abdellani/build-your-own-kafka/app/types"
	"github.com/abdellani/build-your-own-kafka/app/utils"
)

type FetchResponse struct {
	Size int32
	types.ResponseHeaderV1
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
	Reponses       types.COMPACT_ARRAY[FetchResponseReponse]
	types.TAG_BUFFER
}

type FetchResponseReponse struct {
	Topic      types.UUID
	Partitions types.COMPACT_ARRAY[FetchResponsePartition]
	types.TAG_BUFFER
}
type FetchResponsePartition struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  types.COMPACT_ARRAY[FectchResponseAbortedTransaction]
	PreferredReadReplica int32
	Records              types.COMPACT_ARRAY[[]byte]
	types.TAG_BUFFER
}

type FectchResponseAbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
	types.TAG_BUFFER
}
type FetchHandler struct{}

func (h *FetchHandler) Handle(r types.IRequest) types.IResponse {

	req := r.(*FetchRequest)
	cm := cluster_metadata.LoadClusterMetaData(constants.LogFilePath)
	res := FetchResponse{
		Reponses: types.COMPACT_ARRAY[FetchResponseReponse]{},
	}
	for i := 0; i < len(req.Topics); i++ {
		topic := req.Topics[i]
		partitionIndex := req.Topics[i].Partitions[0].Partition
		topicName := cm.FindTopicName(topic.TopicID)
		partition := FetchResponsePartition{
			PartitionIndex: 0,
			ErrorCode:      100,
		}
		if strings.Compare(topicName, "") != 0 {
			logFilePath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partitionIndex)
			_, err := os.Stat(logFilePath)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
				return nil
			}
			partition.ErrorCode = 0
			data, err := os.ReadFile(logFilePath)
			partition.Records = append(partition.Records, data)
		}
		response := FetchResponseReponse{
			Topic:      topic.TopicID,
			Partitions: types.COMPACT_ARRAY[FetchResponsePartition]{partition},
		}
		res.Reponses = append(res.Reponses, response)
	}
	res.PrepareResponse(req)

	return res
}

func (r *FetchResponse) PrepareResponse(req *FetchRequest) {
	r.CorrelationID = req.CorrelationId
	r.UpdateSize()
}
func (r *FetchResponse) UpdateSize() {
	r.Size = utils.CalculateSize(*r) - 4
}

func (r FetchResponse) Serialize() []byte {
	return types.Serialize(r)
}
