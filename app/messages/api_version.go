package messages

import (
	"bytes"
	"encoding/binary"
)

type ApiVersionResponse struct {
	CommonResponseFields
	CorrelationId  int32
	Error          int16
	NumApiKeys     int8
	ApiKeys        []ApiKeys
	ThrottleTimeMs int32
	TAG_BUFFER
}

type ApiKeys struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
	TAG_BUFFER
}

type ApiVersionHandler struct {
	SupportedVersions
}

type TAG_BUFFER int8

func (h *ApiVersionHandler) Handle(req *Request) Response {
	if !req.IsSupportedVersion(int16(h.MinVersion), h.MaxVersion) {
		return NewApiVersionResponse(req.CorrelationId, 35)
	}
	return NewApiVersionResponse(req.CorrelationId, 0)
}

func NewApiVersionResponse(correlationId int32, err int16) *ApiVersionResponse {
	response := ApiVersionResponse{
		CorrelationId: correlationId,
		Error:         err,
		NumApiKeys:    3,
		ApiKeys: []ApiKeys{
			{ApiKey: ApiVersionsApiKey,
				MinVersion: 0,
				MaxVersion: 4},
			{ApiKey: DescribeTopicPartitionsApiKey,
				MinVersion: 0,
				MaxVersion: 0},
		},
		ThrottleTimeMs: 0,
	}
	response.Size = response.CalculateSize()
	return &response
}

func (r *ApiVersionResponse) CalculateSize() int32 {
	return CalculateSize(*r) - 4
}

func (r *ApiVersionResponse) Serialize() []byte {
	buff := bytes.Buffer{}
	binary.Write(&buff, binary.BigEndian, r.Size)
	binary.Write(&buff, binary.BigEndian, r.CorrelationId)
	binary.Write(&buff, binary.BigEndian, r.Error)
	binary.Write(&buff, binary.BigEndian, r.NumApiKeys)
	for i := 0; i < len(r.ApiKeys); i++ {
		ApiKeys := r.ApiKeys[i]
		binary.Write(&buff, binary.BigEndian, ApiKeys)
	}
	binary.Write(&buff, binary.BigEndian, r.ThrottleTimeMs)
	binary.Write(&buff, binary.BigEndian, r.TAG_BUFFER)
	return buff.Bytes()
}
