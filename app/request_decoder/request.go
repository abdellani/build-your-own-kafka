package request_decoder

import (
	"encoding/binary"

	"github.com/abdellani/build-your-own-kafka/app/api_versions"
	"github.com/abdellani/build-your-own-kafka/app/decoder"
	"github.com/abdellani/build-your-own-kafka/app/dtp"
	"github.com/abdellani/build-your-own-kafka/app/fetch"
	"github.com/abdellani/build-your-own-kafka/app/messages"
	"github.com/abdellani/build-your-own-kafka/app/types"
)

func DecodeRequest(bytes []byte) types.IRequest {
	offset := 0
	var size int32
	buffer := bytes[offset : offset+4]
	offset += 4
	size = int32(binary.BigEndian.Uint32(buffer))

	headerV2, offset := types.DeserializeRequestHeaderV2(bytes, offset)

	switch headerV2.ApiKey {
	case messages.API_KEY_API_VERSIONS:
		return &api_versions.APIVersionsRequest{
			Size:            size,
			RequestHeaderV2: *headerV2,
		}
	case messages.API_KEY_DESCRIBE_TOPIC_PARTITIONS:
		DTPBody, _ := dtp.DeserializeDTPBody(bytes, offset)
		return &dtp.DTPRequest{
			Size:            size,
			RequestHeaderV2: *headerV2,
			DTPRequestBody:  *DTPBody,
		}
	case messages.API_KEY_FETCH:
		decoder := &decoder.Decoder{}
		decoder.Init(bytes)
		decoder.SetOffset(int32(offset))
		FetchBody, _ := fetch.DecodeFetchRequestBody(decoder)
		return &fetch.FetchRequest{
			Size:             size,
			RequestHeaderV2:  *headerV2,
			FetchRequestBody: *FetchBody,
		}
	default:
		panic("Inde")
	}
}
