package messages

import (
	"bytes"
	"encoding/binary"
)

type ApiVersionResponse struct {
	Size           int32
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

func (h *ApiVersionHandler) Init() {
	h.SupportedVersions.MinVersion = 0
	h.SupportedVersions.MaxVersion = 4

}

type TAG_BUFFER int8

func (h *ApiVersionHandler) Handle(req *Request) Response {
	if !req.IsSupportedVersion(h.SupportedVersions.MinVersion, h.SupportedVersions.MaxVersion) {
		return NewApiVersionResponse(req.CorrelationId, 35)
	}
	return NewApiVersionResponse(req.CorrelationId, 0)
}

func NewApiVersionResponse(correlationId int32, err int16) *ApiVersionResponse {
	response := ApiVersionResponse{
		Size:          0,
		CorrelationId: correlationId,
		Error:         err,
		NumApiKeys:    2,
		ApiKeys: []ApiKeys{
			{ApiKey: 18,
				MinVersion: 0,
				MaxVersion: 4},
		},
		ThrottleTimeMs: 0,
	}
	response.Size = response.CalculateSize()
	return &response
}

func (r *ApiVersionResponse) CalculateSize() int32 {
	const (
		CorrelationId  = 4
		Error          = 2
		NumApiKeys     = 1 //TODO: this is a VARINT not constant size int
		ApiKeys        = 7
		ThrottleTimeMs = 4
		TAG_BUFFER     = 1
	)

	return int32(CorrelationId + Error +
		NumApiKeys + len(r.ApiKeys)*ApiKeys +
		ThrottleTimeMs +
		TAG_BUFFER)
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
