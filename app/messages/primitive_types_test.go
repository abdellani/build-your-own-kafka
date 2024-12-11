package messages_test

import (
	"reflect"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/messages"
)

func TestDeserializeRequestHeaderV2(t *testing.T) {
	var RequestCommonField = []byte{
		0x00, 0x00, 0x00, 0x14, //Size
	}
	var RequestHeaderV2Example = []byte{
		//Request Header
		0x00, 0x4b, //ApiKey (int16)
		0x00, 0x04, //ApiVersion (int16)
		0x24, 0x35, 0x46, 0x57, //CorrelationId (int32)
		//ClientId
		0x00, 0x09, //N (int16)
		0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, //String ([]byte)
		0x00, //TAG_BUFFER (byte)
	}
	payload := append(RequestCommonField, RequestHeaderV2Example...)
	got, got_offset := messages.DeserializeRequestHeaderV2(payload, 4)
	want_offset := 24
	want := &messages.RequestHeaderV2{
		ApiKey:        75,
		ApiVersion:    4,
		CorrelationId: 0x24354657,
		ClientId:      messages.NULLABLE_STRING{0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69},
		TAG_BUFFER:    0,
	}
	if got_offset != want_offset {
		t.Errorf("wrong offset, got %d, want %d", got_offset, want_offset)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Serialization Error, got %+v,want %+v", got, want)
	}
}

func TestCOMPACT_ARRAY(t *testing.T) {
	t.Run("Serialize an array of int16", func(t *testing.T) {
		data := messages.COMPACT_ARRAY[int16]{
			1, 2, 3, 4, 5,
		}
		got := data.Serialize()
		want := []byte{6, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Serialization error, got %v want %v", got, want)
		}
	})

	t.Run("Serialize an array of []int16", func(t *testing.T) {
		data := messages.COMPACT_ARRAY[[]int16]{
			[]int16{1, 2, 3, 10, 11, 12},
		}
		got := data.Serialize()
		want := []byte{2, 0, 1, 0, 2, 0, 3, 0, 10, 0, 11, 0, 12}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Serialization error, got %v want %v", got, want)
		}
	})

	t.Run("serialize a COMPACT_ARRAY[struct]", func(t *testing.T) {
		data := messages.COMPACT_ARRAY[struct {
			ID             []byte
			ThrottleTimeMs int16
		}]{
			{ID: []byte{0x49, 0x50, 0x51}, ThrottleTimeMs: 30},
			{ID: []byte{0x25, 0x98, 0x10}, ThrottleTimeMs: 15},
		}
		got := messages.Serialize(data)
		want := []byte{
			0x3,
			0x49, 0x50, 0x51,
			0x00, 0x1E,
			0x25, 0x98, 0x10,
			0x00, 0x0F,
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("wrong serialization of COMPACT_ARRAY, got %v, want %v", got, want)
		}
	})
}

func TestCOMPACT_STRING(t *testing.T) {
	t.Run("Serialize", func(t *testing.T) {
		data := messages.COMPACT_STRING{0x10, 0x11, 0x12, 0x13}
		got := data.Serialize()
		want := []byte{0x05, 0x10, 0x11, 0x12, 0x13}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("COMPACT_STRING serialization error, got %v,want %v", got, want)
		}
	})
}

func TestNULLABLE_STRING(t *testing.T) {
	t.Run("NULL Serialize", func(t *testing.T) {
		data := messages.NULLABLE_STRING{}
		got := data.Serialize()
		want := []byte{0xFF, 0xff}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Serialization error, got %v, want %v", got, want)
		}
	})
	t.Run("NON-EMPTY Serialize", func(t *testing.T) {
		data := messages.NULLABLE_STRING{0x61, 0x62, 0x63}
		got := data.Serialize()
		want := []byte{0x0, 0x3, 0x61, 0x62, 0x63}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Serialization error, got %v, want %v", got, want)
		}
	})
}
