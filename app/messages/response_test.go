package messages_test

import (
	"reflect"
	"testing"

	"github.com/abdellani/build-your-own-kafka/app/api_versions"
	"github.com/abdellani/build-your-own-kafka/app/dtp"
	"github.com/abdellani/build-your-own-kafka/app/messages"
	"github.com/abdellani/build-your-own-kafka/app/types"
	"github.com/abdellani/build-your-own-kafka/app/utils"
)

func TestCalculateSize(t *testing.T) {
	t.Run("DTPResponse", func(t *testing.T) {
		payload := dtp.DTPResponse{ //Total 43 bytes
			Size: 1, // 4
			ResponseHeaderV1: types.ResponseHeaderV1{ // Total 5 bytes
				CorrelationID: 1, //4
				TAG_BUFFER:    0, //1
			},
			ThrottleTimeMs: 0, //4
			Topics: types.COMPACT_ARRAY[dtp.Topic]{ //Total 28 bytes
				//1 for length
				{
					ErrorCode:  0,            //2
					Name:       []byte{0},    //1+1
					TopicId:    types.UUID{}, //16
					IsInternal: 0,            //1
					// 1 for partitions length
					TopicsAuthorizedOperations: 0, //4
					TAG_BUFFER:                 0, //1
				},
			},
			NextCursor: types.NULLABLE_FIELD[dtp.NextCursor]{
				IsNull: true, //1 because it'll be null
			},
			TAG_BUFFER: 0, // 1
		}
		got := utils.CalculateSize(payload)
		var wanted int32 = 43
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
			got := types.Serialize(c.Num)
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

		got := types.Serialize(object)
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
			UUID           types.UUID
			CorrelationId  int32
			Error          int16
			ThrottleTimeMs int32
		}{
			Size:       27,
			NumApiKeys: 0,
			UUID: types.UUID{
				0x19, 0x58, 0x23, 0xAB,
				0xC5, 0xEF, 0xF4, 0xA2,
				0x51, 0xAE, 0xDE, 0x5B,
				0x34, 0x22, 0x94, 0x31,
			},
			CorrelationId:  1897568,
			Error:          14589,
			ThrottleTimeMs: 6542312,
		}
		got := types.Serialize(object)
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
		object := api_versions.ApiVersionsResponse{
			Size: 26,
			ResponseHeaderV0: types.ResponseHeaderV0{
				CorrelationID: 78921354,
			},
			Error:      16845,
			NumApiKeys: 3,
			ApiKeys: []api_versions.ApiKeys{
				{ApiKey: messages.API_KEY_API_VERSIONS,
					MinVersion: 0,
					MaxVersion: 4,
					TAG_BUFFER: 0,
				},
				{ApiKey: messages.API_KEY_DESCRIBE_TOPIC_PARTITIONS,
					MinVersion: 0,
					MaxVersion: 0,
					TAG_BUFFER: 0,
				},
			},
			ThrottleTimeMs: 648246214,
			TAG_BUFFER:     0,
		}

		got := types.Serialize(object)
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
