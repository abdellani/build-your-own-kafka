package messages

import (
	"encoding/binary"
)

type DTPRequest struct {
	Size int32
	RequestHeaderV2
	DTPRequestBody
}

type DTPRequestBody struct {
	Topics                 COMPACT_ARRAY[DTPRequestTopic]
	ResponsePartitionLimit int32
	Cursor                 DTPRequestCursor
	TAG_BUFFER
}
type DTPRequestTopic struct {
	Name COMPACT_STRING
	TAG_BUFFER
}
type DTPRequestCursor struct {
	TopicName      COMPACT_STRING
	PartitionIndex int32
	TAG_BUFFER
}

func DeserializeDTPBody(data []byte, offset int) (*DTPRequestBody, int) {
	//Request Body
	body := &DTPRequestBody{}
	buffer := data[offset : offset+1]
	offset += 1
	body.Topics.N = buffer
	for i := byte(1); i < body.Topics.N[0]; i++ {
		/*
			COMPACT_STRING {
				N UNSIGNED_VARINT (+1)
				String []byte
			}
		*/
		item := DTPRequestTopic{}
		buffer = data[offset : offset+1]
		offset += 1
		item.Name.N = buffer
		buffer = data[offset : offset+int(item.Name.N[0]-1)]
		offset += int(item.Name.N[0] - 1)
		item.Name.String = buffer
		buffer = data[offset : offset+1]
		offset += 1
		item.TAG_BUFFER = TAG_BUFFER(buffer[0])
		body.Topics.Items = append(body.Topics.Items, item)
	}
	buffer = data[offset : offset+4]
	offset += 4
	body.ResponsePartitionLimit = int32(binary.BigEndian.Uint32(buffer))
	buffer = data[offset : offset+1]
	offset += 1
	body.Cursor.TopicName.N = buffer
	buffer = data[offset : offset+int(body.Cursor.TopicName.N[0])]
	offset += int(body.Cursor.TopicName.N[0])

	body.Cursor.TopicName.String = buffer
	buffer = data[offset : offset+4]
	offset += 4
	body.Cursor.PartitionIndex = int32(binary.BigEndian.Uint32(buffer))
	buffer = data[offset : offset+1]
	offset += 1
	body.Cursor.TAG_BUFFER = TAG_BUFFER(buffer[0])
	buffer = data[offset : offset+1]
	offset += 1
	body.TAG_BUFFER = TAG_BUFFER(buffer[0])

	return body, offset
}
