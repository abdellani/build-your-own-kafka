package main

import (
	"github.com/abdellani/build-your-own-kafka/app/cluster_metadata"
	"github.com/abdellani/build-your-own-kafka/app/decoder"
)

const path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

func main() {
	decoder := &decoder.Decoder{}
	cm := &cluster_metadata.ClusterMetadata{}
	decoder.LoadBinaryFile(path)
	cm.Init(decoder)
	cm.Decode()
}
