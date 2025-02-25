package cluster_metadata

import (
	"github.com/abdellani/build-your-own-kafka/app/encoder"
	"github.com/abdellani/build-your-own-kafka/app/types"
)

func (v ValuePartitionRecord) Encode(e *encoder.Encoder) {
	e.PutInt32(v.PartitionID)
	e.PutBytes(v.TopicUUID[:])
	e.PutBytes(v.ReplicaArray.Serialize())
	e.PutBytes(v.InSyncReplicaArray.Serialize())
	e.PutBytes(v.RemovingReplicaArray.Serialize())
	e.PutBytes(v.AddingReplicaArray.Serialize())
	e.PutInt32(v.Leader)
	e.PutInt32(v.LeaderEpoch)
	e.PutInt32(v.PartitionEpoch)
	e.PutBytes(v.DirectoriesArray.Serialize())
	e.PutVarint(types.SIGNED_VARINT(v.TAG_BUFFER))
}
