package handler

import (
	"github.com/abdellani/build-your-own-kafka/app/api_versions"
	"github.com/abdellani/build-your-own-kafka/app/dtp"
	"github.com/abdellani/build-your-own-kafka/app/fetch"
	"github.com/abdellani/build-your-own-kafka/app/messages"
	"github.com/abdellani/build-your-own-kafka/app/types"
)

type Handler interface {
	Handle(types.IRequest) types.IResponse
}

func HandleRequest(req types.IRequest) types.IResponse {
	var handler Handler
	switch req.ApiKeyValue() {
	case messages.API_KEY_API_VERSIONS, messages.API_KEY_DESCRIBE_TOPIC_PARTITIONS, messages.API_KEY_FETCH:
		handler = Handlers[req.ApiKeyValue()]
	default:
		handler = Handlers[messages.API_KEY_API_VERSIONS]
	}
	response := handler.Handle(req)
	return response
}

var Handlers = map[int16]Handler{
	messages.API_KEY_API_VERSIONS: &api_versions.ApiVersionsHandler{
		SupportedVersions: messages.SupportedVersions{
			MinVersion: 0,
			MaxVersion: 4,
		},
	},
	messages.API_KEY_DESCRIBE_TOPIC_PARTITIONS: &dtp.DTPHandler{
		SupportedVersions: messages.SupportedVersions{
			MinVersion: 0,
			MaxVersion: 0,
		},
	},
	messages.API_KEY_FETCH: &fetch.FetchHandler{},
}
