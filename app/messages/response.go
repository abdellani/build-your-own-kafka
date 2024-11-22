package messages

const (
	ApiVersionsApiKey             = 18
	DescribeTopicPartitionsApiKey = 75
)

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
	handler.Init()
	response := handler.Handle(req)
	return response
}

var Handlers = map[int16]Handler{
	ApiVersionsApiKey:             &ApiVersionHandler{},
	DescribeTopicPartitionsApiKey: nil,
}

type Handler interface {
	Init()
	Handle(*Request) Response
}
type Response interface {
	Serialize() []byte
}
