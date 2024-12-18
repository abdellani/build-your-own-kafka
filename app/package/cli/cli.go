package main

import (
	"github.com/codecrafters-io/kafka-starter-go/app/messages"
)

const path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

func main() {
	decoder := &messages.Decoder{}
	cm := &messages.ClusterMetadata{}
	decoder.LoadBinaryFile(path)
	cm.Init(decoder)
	cm.Decode()
}
