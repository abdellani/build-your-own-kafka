package messages_test

import (
	"reflect"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/messages"
)

func TestDeserializeRequest(t *testing.T) {
	t.Run("DTP Request", func(t *testing.T) {
		data := []byte{
			0, 0, 0, 37, //Size
			//Header
			0, 75, //ApiKey
			0, 0, //ApiVersion
			0x27, 0x0A, 0x52, 0xD0, //Correlation Id
			0x00, 0x09, //Client ID length = 0
			0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, //Kafka
			0x00, //TAG
			//Body
			0x02,                   //->Topics(array length)
			0x04, 0x66, 0x6f, 0x6f, //->Topics-> (length+1),Name
			0x00,                   //->Topics->TAG_BUFFER
			0x00, 0x00, 0x43, 0x21, //ResponsePratitionLimit
			//Cursor
			0x00,                   //Cursor-> Topic Name
			0x21, 0xB2, 0xC1, 0x2D, //Cursor->PartitionIndex
			0x00, //Cursor->TAG_BUFFER
			0x00, //TAG Buffer
		}

		got := messages.DeserializeRequest(data)

		want := &messages.DTPRequest{
			Size: 37,
			RequestHeaderV2: messages.RequestHeaderV2{
				ApiKey:        75,
				ApiVersion:    0,
				CorrelationId: 0x270A52D0,
				ClientId: messages.NULLABLE_STRING{
					N:      9,
					String: []byte{0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69},
				},
			},
			DTPRequestBody: messages.DTPRequestBody{
				Topics: messages.COMPACT_ARRAY[messages.DTPRequestTopic]{
					{
						Name: messages.COMPACT_STRING{
							N:      []byte{4},
							String: []byte{0x66, 0x6f, 0x6f},
						},
					},
				},
				ResponsePartitionLimit: 0x4321,
				Cursor: messages.DTPRequestCursor{
					TopicName:      messages.COMPACT_STRING{N: []byte{0}, String: []byte{}},
					PartitionIndex: 0x21B2C12D,
					TAG_BUFFER:     messages.TAG_BUFFER(0),
				},
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("wrong DTPBody, got \n%+v \nwant \n%+v", got, want)
		}
	})
}
