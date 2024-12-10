package messages

import (
	"encoding/binary"
)

func (r *RequestHeaderV2) IsSupportedVersion(min, max int16) bool {
	return min <= r.ApiVersion &&
		r.ApiVersion <= max
}

func DeserializeRequest(bytes []byte) IRequest {
	offset := 0
	var size int32
	buffer := bytes[offset : offset+4]
	offset += 4
	size = int32(binary.BigEndian.Uint32(buffer))

	headerV2, offset := DeserializeRequestHeaderV2(bytes, offset)

	switch headerV2.ApiKey {
	case API_KEY_API_VERSIONS:
		return &APIVersionsRequest{
			Size:            size,
			RequestHeaderV2: *headerV2,
		}
	case API_KEY_DESCRIBE_TOPIC_PARTITIONS:
		DTPBody, _ := DeserializeDTPBody(bytes, offset)
		return &DTPRequest{
			Size:            size,
			RequestHeaderV2: *headerV2,
			DTPRequestBody:  *DTPBody,
		}
	default:
		panic("Inde")
	}
}
