package utils

import "github.com/abdellani/build-your-own-kafka/app/types"

func CalculateSize(data types.ISerializable) int32 {
	//TODO avoid serializing twice
	return int32(len(data.Serialize()))
}
