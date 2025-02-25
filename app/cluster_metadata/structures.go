package cluster_metadata

import (
	"github.com/abdellani/build-your-own-kafka/app/decoder"
	"github.com/abdellani/build-your-own-kafka/app/encoder"
	"github.com/abdellani/build-your-own-kafka/app/types"
)

type ClusterMetadata struct {
	RecordBatchs types.COMPACT_ARRAY[RecordBatch]
	decoder      *decoder.Decoder
}

type Value struct {
	FrameVersion int8
	Type         int8
	Version      int8
	Details      ValueDetails
}

type ValueDetails interface {
	Encode(e *encoder.Encoder)
}

const VALUE_TYPE_TOPIC_RECORD = 2
const VALUE_TYPE_PARTITION_RECORD = 3

type ValueTopicsRecord struct {
	TopicName types.COMPACT_STRING
	TopicUUID types.UUID
	types.TAG_BUFFER
}

type ValuePartitionRecord struct {
	PartitionID          int32
	TopicUUID            types.UUID
	ReplicaArray         types.COMPACT_ARRAY[int32]
	InSyncReplicaArray   types.COMPACT_ARRAY[int32]
	RemovingReplicaArray types.COMPACT_ARRAY[int32]
	AddingReplicaArray   types.COMPACT_ARRAY[int32]
	Leader               int32
	LeaderEpoch          int32
	PartitionEpoch       int32
	DirectoriesArray     types.COMPACT_ARRAY[types.UUID]
	types.TAG_BUFFER
}
