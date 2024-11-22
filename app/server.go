package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
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

type Request struct {
	MessageSize       int32
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
}

func (r *Request) IsSupportedVersion() bool {
	supportedVersions := map[int]struct {
		MinVersion int16
		MaxVersion int16
	}{
		18: {
			MinVersion: 0,
			MaxVersion: 4,
		},
	}
	supportedVersion := supportedVersions[int(r.RequestApiKey)]
	return supportedVersion.MinVersion <= r.RequestApiVersion &&
		r.RequestApiVersion <= supportedVersion.MaxVersion
}

type Response struct {
	Size           int32
	CorrelationId  int32
	Error          int16
	NumApiKeys     int8
	ApiKeys        ApiKeys
	ThrottleTimeMs int32
	TAG_BUFFER
}

type ApiKeys struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
	TAG_BUFFER
}

type TAG_BUFFER int8

func DeserializeRequest(b bytes.Buffer) *Request {
	r := Request{}
	reader := bytes.NewReader(b.Bytes())
	binary.Read(reader, binary.BigEndian, &r)
	return &r
}

func NewResponse(correlationId int32, err int16) *Response {
	response := Response{
		Size:          0,
		CorrelationId: correlationId,
		Error:         err,
		NumApiKeys:    2,
		ApiKeys: ApiKeys{
			ApiKey:     18,
			MinVersion: 0,
			MaxVersion: 4,
		},
		ThrottleTimeMs: 0,
	}
	response.Size = int32(binary.Size(&response)) - 4
	return &response
}

func serializeResponse(r *Response) []byte {
	buff := bytes.Buffer{}
	binary.Write(&buff, binary.BigEndian, r)
	return buff.Bytes()
}

func handleConnection(c net.Conn) {
	defer c.Close()
	received := bytes.Buffer{}
	buff := make([]byte, 1024)
	c.Read(buff)
	received.Write(buff)
	request := DeserializeRequest(received)
	if !request.IsSupportedVersion() {
		response := NewResponse(request.CorrelationId, 35)
		c.Write(serializeResponse(response))

	}
	response := NewResponse(request.CorrelationId, 0)
	c.Write(serializeResponse(response))
}
