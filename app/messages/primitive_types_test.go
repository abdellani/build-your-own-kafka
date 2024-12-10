package messages_test

import (
	"reflect"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/messages"
)

func TestDeserializeRequestHeaderV2(t *testing.T) {

	payload := []byte{
		0x00, 0x00, 0x00, 0x13, //Size
		//Request Header
		0x00, 0x4b, //ApiKey (int16)
		0x00, 0x04, //ApiVersion (int16)
		0x24, 0x35, 0x46, 0x57, //CorrelationId (int32)
		//ClientId
		0x00, 0x09, //N (int16)
		0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, //String ([]byte)
		0x00, //TAG_BUFFER (byte)
	}
	got, got_offset := messages.DeserializeRequestHeaderV2(payload, 4)
	want_offset := 24
	want := &messages.RequestHeaderV2{
		ApiKey:        75,
		ApiVersion:    4,
		CorrelationId: 0x24354657,
		ClientId: messages.NULLABLE_STRING{
			N:      9,
			String: []byte{0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69},
		},
		TAG_BUFFER: 0,
	}
	if got_offset != want_offset {
		t.Errorf("wrong offset, got %d, want %d", got_offset, want_offset)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Serialization Error, got %+v,want %+v", got, want)
	}
}
