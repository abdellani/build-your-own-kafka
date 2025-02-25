package cluster_metadata

import "github.com/abdellani/build-your-own-kafka/app/encoder"

func (v ValueTopicsRecord) Encode(e *encoder.Encoder) {
	e.PutBytes(v.TopicName.Serialize())
	e.PutBytes(v.TopicUUID[:])
	e.PutVarint(0)
}
