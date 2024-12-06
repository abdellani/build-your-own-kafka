package messages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
)

const (
	ApiVersionsApiKey             = 18
	DescribeTopicPartitionsApiKey = 75
)

type ResponseHeaderV0 struct {
	CorrelationID int32
}
type ResponseHeaderV1 struct {
	CorrelationID int32
	TAG_BUFFER
}

type SupportedVersions struct {
	MinVersion int16
	MaxVersion int16
}

func HandleRequest(req *RequestHeaderV0) Response {
	var handler Handler
	switch req.ApiKey {
	case ApiVersionsApiKey, DescribeTopicPartitionsApiKey:
		handler = Handlers[req.ApiKey]
	default:
		panic(errors.New(`unsupported ApiKey request`))
	}
	response := handler.Handle(req)
	return response
}

var Handlers = map[int16]Handler{
	ApiVersionsApiKey: &ApiVersionsHandler{
		SupportedVersions: SupportedVersions{
			MinVersion: 0,
			MaxVersion: 4,
		},
	},
	DescribeTopicPartitionsApiKey: &DTPHandler{
		SupportedVersions: SupportedVersions{
			MinVersion: 0,
			MaxVersion: 0,
		},
	},
}

type Handler interface {
	Handle(*RequestHeaderV0) Response
}
type Response interface {
	Serialize() []byte
}

func CalculateSize(data any) int32 {
	var size int32 = 0
	dataType := reflect.TypeOf(data)
	kind := dataType.Kind()
	switch kind {
	// byte is an alias of Uint8
	case reflect.Int32, reflect.Int16, reflect.Int8, reflect.Uint8:
		return int32(dataType.Size())
	case reflect.Struct:
		value := reflect.ValueOf(data)
		for i := 0; i < value.NumField(); i++ {
			size += CalculateSize(value.Field(i).Interface())
		}
	case reflect.Slice, reflect.Array:
		value := reflect.ValueOf(data)
		for i := 0; i < value.Len(); i++ {
			size += CalculateSize(value.Index(i).Interface())
		}
	}
	return size
}

func Serialize(data any) []byte {
	buff := bytes.Buffer{}
	dataType := reflect.TypeOf(data)
	kind := dataType.Kind()
	switch kind {
	// byte is an alias of Uint8
	case reflect.Int32, reflect.Int16, reflect.Int8, reflect.Uint8:
		binary.Write(&buff, binary.BigEndian, data)
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
	}
	return buff.Bytes()
}
