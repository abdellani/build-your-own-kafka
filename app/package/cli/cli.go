package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/messages"
)

const path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

func main() {
	content, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", content)
	decoder := messages.Decoder{}
	decoder.Init(content)
	// metadata := messages.DecodeClusterMetada(&decoder)
	// fmt.Printf("%+v\n", metadata)
	i, n := binary.Varint([]byte{0x02})
	fmt.Printf("n %d n %d\n", i, n)
}
