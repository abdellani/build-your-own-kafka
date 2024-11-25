package messages_test

import (
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
