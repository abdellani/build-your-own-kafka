package main

import (
	"bytes"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/messages"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(c net.Conn) {
	defer c.Close()
	received := bytes.Buffer{}
	buff := make([]byte, 1024)
	c.Read(buff)
	received.Write(buff)
	request := messages.DeserializeRequest(received)
	response := messages.HandleRequest(request)
	c.Write(response.Serialize())
}
