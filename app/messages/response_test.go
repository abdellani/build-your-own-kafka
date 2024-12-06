package messages_test

import (
	"reflect"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/messages"
)

func TestCalculateSize(t *testing.T) {
	t.Run("integers", func(t *testing.T) {
		cases := []struct {
			num  any
			want int32
		}{
			{int8(10), 1},
			{int16(13), 2},
			{int32(66), 4},
		}
		for _, c := range cases {
			got := messages.CalculateSize(c.num)
			var wanted int32 = c.want
			if got != wanted {
				t.Errorf("expected %d, got %d", wanted, got)
			}
		}

	})
	t.Run("structure of integers", func(t *testing.T) {
		object := struct {
			NumApiKeys     int8
			CorrelationId  int32
			Error          int16
			ThrottleTimeMs int32
		}{
			NumApiKeys: 3,
		}
		got := messages.CalculateSize(object)
		var wanted int32 = 11 // 1 + 4 + 2 + 4
		if got != wanted {
			t.Errorf("expected %d, got %d", wanted, got)
		}
	})
	t.Run("nested structures", func(t *testing.T) {
		object := struct {
			Shared struct {
				Size int32
			}
			NumApiKeys     int8
			CorrelationId  int32
			Error          int16
			ThrottleTimeMs int32
		}{}
		got := messages.CalculateSize(object)
		var wanted int32 = 15 //4 + 1 + 4 + 2 + 4
		if got != wanted {
			t.Errorf("expected %d, got %d", wanted, got)
		}

	})

	t.Run("a structure that contains a Slice", func(t *testing.T) {
		object := struct {
			Size           int32
			NumApiKeys     int8
			ApiKeys        []messages.ApiKeys
			CorrelationId  int32
			Error          int16
			ThrottleTimeMs int32
		}{
			ApiKeys: []messages.ApiKeys{
				{
					ApiKey:     18,
					MinVersion: 0,
				},
				{
					ApiKey: 75,
				},
			},
		}
		got := messages.CalculateSize(object)
		var wanted int32 = 29 //4 + 1 + 2*(2+2+2+1) + 4 + 2 + 4
		if got != wanted {
			t.Errorf("expected %d, got %d", wanted, got)
		}
	})

	t.Run("a structure that contains an Array", func(t *testing.T) {
		object := struct {
			Size           int32
			NumApiKeys     int8
			UUID           messages.UUID
			CorrelationId  int32
			Error          int16
			ThrottleTimeMs int32
		}{}
		got := messages.CalculateSize(object)
		var wanted int32 = 31 // 4 + 1 + 16*(1) + 4 + 2 + 4
		if got != wanted {
			t.Errorf("expected %d, got %d", wanted, got)
		}

	})
}

func TestSerialize(t *testing.T) {
	t.Run("Integers", func(t *testing.T) {
		cases := []struct {
			Num  any
			Want []byte
		}{
			{Num: int32(5427), Want: []byte{0, 0, 0x15, 0x33}},
			{Num: int16(1954), Want: []byte{0x07, 0xA2}},
		}
		for _, c := range cases {
			got := messages.Serialize(c.Num)
			if !reflect.DeepEqual(got, c.Want) {
				t.Errorf("got %v, want %v", got, c.Want)
			}
		}
	})
	t.Run("a struct composed of integers", func(t *testing.T) {
		object := struct {
			Size          byte
			CorrelationId int32
			Error         int16
		}{
			Size:          6,
			CorrelationId: 1789,
			Error:         38,
		}

		got := messages.Serialize(object)
		want := []byte{
			6,               //Size
			0, 0, 0x6, 0xFD, //CorrelationId
			0, 0x26, //Error
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("a struct composed of intergers and Arrays", func(t *testing.T) {
		object := struct {
			Size           int32
			NumApiKeys     int8
			UUID           messages.UUID
			CorrelationId  int32
			Error          int16
			ThrottleTimeMs int32
		}{
			Size:       27,
			NumApiKeys: 0,
			UUID: messages.UUID{
				0x19, 0x58, 0x23, 0xAB,
				0xC5, 0xEF, 0xF4, 0xA2,
				0x51, 0xAE, 0xDE, 0x5B,
				0x34, 0x22, 0x94, 0x31,
			},
			CorrelationId:  1897568,
			Error:          14589,
			ThrottleTimeMs: 6542312,
		}
		got := messages.Serialize(object)
		want := []byte{
			0, 0, 0, 0x1B, //Size
			0, //NumApiKeys
			0x19, 0x58, 0x23, 0xAB,
			0xC5, 0xEF, 0xF4, 0xA2,
			0x51, 0xAE, 0xDE, 0x5B,
			0x34, 0x22, 0x94, 0x31, //UUID
			0, 0x1C, 0xF4, 0x60, //CorrelationID
			0x38, 0xFD, //Error
			0, 0x63, 0xD3, 0xE8, //ThrottleTimeMs
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("a struct compose of intergers and Slices", func(t *testing.T) {
		object := messages.ApiVersionsResponse{
			Size: 26,
			ResponseHeaderV0: messages.ResponseHeaderV0{
				CorrelationID: 78921354,
			},
			Error:      16845,
			NumApiKeys: 3,
			ApiKeys: []messages.ApiKeys{
				{ApiKey: messages.ApiVersionsApiKey,
					MinVersion: 0,
					MaxVersion: 4,
					TAG_BUFFER: 0,
				},
				{ApiKey: messages.DescribeTopicPartitionsApiKey,
					MinVersion: 0,
					MaxVersion: 0,
					TAG_BUFFER: 0,
				},
			},
			ThrottleTimeMs: 648246214,
			TAG_BUFFER:     0,
		}

		got := messages.Serialize(object)
		want := []byte{
			0, 0, 0, 0x1A, //Size
			0x04, 0xB4, 0x3E, 0x8A, //CorrelationId
			0x41, 0xCD, //Error
			0x3,                    //NumApiKeys
			0, 0x12, 0, 0, 0, 4, 0, //ApiKeys[0]
			0, 0x4B, 0, 0, 0, 0, 0, //ApiKeys[1]
			0x26, 0xA3, 0x73, 0xC6, 0, //ThrottleTimeMs
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %x, want %x", got, want)
		}
	})
}
