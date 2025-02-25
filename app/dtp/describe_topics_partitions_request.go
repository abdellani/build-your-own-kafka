package dtp

import (
	"encoding/binary"

	"github.com/abdellani/build-your-own-kafka/app/types"
)

type DTPRequest struct {
	Size int32
	types.RequestHeaderV2
	DTPRequestBody
}

type DTPRequestBody struct {
	Topics                 types.COMPACT_ARRAY[DTPRequestTopic]
	ResponsePartitionLimit int32
	Cursor                 DTPRequestCursor
	types.TAG_BUFFER
}
type DTPRequestTopic struct {
	Name types.COMPACT_STRING
	types.TAG_BUFFER
}
type DTPRequestCursor struct {
	TopicName      types.COMPACT_STRING
	PartitionIndex int32
	types.TAG_BUFFER
}

func DeserializeDTPBody(data []byte, offset int) (*DTPRequestBody, int) {
	//Request Body
	body := &DTPRequestBody{}
	buffer := data[offset : offset+1]
	offset += 1
	N := buffer
	for i := byte(1); i < N[0]; i++ {
		/*
			COMPACT_STRING {
				N UNSIGNED_VARINT (+1)
				String []byte
			}
		*/
		item := DTPRequestTopic{}
		buffer = data[offset : offset+1]
		offset += 1
		n := buffer
		buffer = data[offset : offset+int(n[0]-1)]
		offset += int(n[0] - 1)
		item.Name = buffer
		buffer = data[offset : offset+1]
		offset += 1
		item.TAG_BUFFER = types.TAG_BUFFER(buffer[0])
		body.Topics = append(body.Topics, item)
	}
	buffer = data[offset : offset+4]
	offset += 4
	body.ResponsePartitionLimit = int32(binary.BigEndian.Uint32(buffer))

	buffer = data[offset : offset+1]
	offset += 1
	n := buffer
	buffer = data[offset : offset+int(n[0])]
	offset += int(n[0])
	body.Cursor.TopicName = buffer

	buffer = data[offset : offset+4]
	offset += 4
	body.Cursor.PartitionIndex = int32(binary.BigEndian.Uint32(buffer))
	buffer = data[offset : offset+1]
	offset += 1
	body.Cursor.TAG_BUFFER = types.TAG_BUFFER(buffer[0])
	buffer = data[offset : offset+1]
	offset += 1
	body.TAG_BUFFER = types.TAG_BUFFER(buffer[0])

	return body, offset
}
