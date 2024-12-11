package messages

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

const (
	API_KEY_API_VERSIONS              = 18
	API_KEY_DESCRIBE_TOPIC_PARTITIONS = 75
)

type SupportedVersions struct {
	MinVersion int16
	MaxVersion int16
}
type IRequest interface {
	ApiKeyValue() int16
}

type Handler interface {
	Handle(IRequest) IResponse
}

type IResponse interface {
	Serialize() []byte
}

func HandleRequest(req IRequest) IResponse {
	var handler Handler
	switch req.ApiKeyValue() {
	case API_KEY_API_VERSIONS, API_KEY_DESCRIBE_TOPIC_PARTITIONS:
		handler = Handlers[req.ApiKeyValue()]
	default:
		handler = Handlers[API_KEY_API_VERSIONS]
	}
	response := handler.Handle(req)
	return response
}

var Handlers = map[int16]Handler{
	API_KEY_API_VERSIONS: &ApiVersionsHandler{
		SupportedVersions: SupportedVersions{
			MinVersion: 0,
			MaxVersion: 4,
		},
	},
	API_KEY_DESCRIBE_TOPIC_PARTITIONS: &DTPHandler{
		SupportedVersions: SupportedVersions{
			MinVersion: 0,
			MaxVersion: 0,
		},
	},
}

func CalculateSize(data ISerializable) int32 {
	//TODO avoid serializing twice
	return int32(len(data.Serialize()))
}

func Serialize(data any) []byte {
	buff := bytes.Buffer{}
	serializable, ok := data.(ISerializable)
	primitive, ok2 := data.(IPrimitiveType)
	if ok && ok2 && primitive.isPrimitiveType() {
		binary.Write(&buff, binary.BigEndian, serializable.Serialize())
		return buff.Bytes()
	}
	dataType := reflect.TypeOf(data)
	kind := dataType.Kind()
	switch kind {
	// byte is an alias of Uint8
	case reflect.Int32, reflect.Int16, reflect.Int8, reflect.Uint8:
		err := binary.Write(&buff, binary.BigEndian, data)
		if err != nil {
			panic(err)
		}
	case reflect.Struct:
		value := reflect.ValueOf(data)
		for i := 0; i < value.NumField(); i++ {
			binary.Write(&buff, binary.BigEndian, Serialize(value.Field(i).Interface()))
		}
	case reflect.Slice, reflect.Array:
		value := reflect.ValueOf(data)
		for i := 0; i < value.Len(); i++ {
			binary.Write(&buff, binary.BigEndian, Serialize(value.Index(i).Interface()))
		}
	case reflect.Int:
		panic("Should not use int type, because the system change depending on the architecture")
	default:
		panic(fmt.Sprintf("Kind not support in serialization, %v", kind))
	}
	return buff.Bytes()
}
