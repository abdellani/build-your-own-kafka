package messages

const (
	ApiVersionsApiKey             = 18
	DescribeTopicPartitionsApiKey = 75
)

type CommonResponseFields struct {
	Size int32
}

func (r *CommonResponseFields) CalculateSize() int32 {
	return 0
}

func (r *CommonResponseFields) Serialize() []byte {
	return nil
}

type SupportedVersions struct {
	MinVersion int16
	MaxVersion int16
}

func HandleRequest(req *Request) Response {
	var handler Handler
	switch req.ApiKey {
	case ApiVersionsApiKey, DescribeTopicPartitionsApiKey:
		handler = Handlers[req.ApiKey]
	}
	response := handler.Handle(req)
	return response
}

var Handlers = map[int16]Handler{
	ApiVersionsApiKey: &ApiVersionHandler{
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
	Handle(*Request) Response
}
type Response interface {
	Serialize() []byte
}
