package messages

type ApiVersionsResponse struct {
	Size int32
	ResponseHeaderV0
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

type ApiVersionsHandler struct {
	SupportedVersions
}

type TAG_BUFFER int8

func (h *ApiVersionsHandler) Handle(req IRequest) Response {
	r := req.(*RequestHeaderV2)
	if !r.IsSupportedVersion(int16(h.MinVersion), h.MaxVersion) {
		return NewApiVersionsResponse(r.CorrelationId, 35)
	}
	return NewApiVersionsResponse(r.CorrelationId, 0)
}

func NewApiVersionsResponse(correlationId int32, err int16) *ApiVersionsResponse {
	response := ApiVersionsResponse{
		Error:      err,
		NumApiKeys: 3,
		ApiKeys: []ApiKeys{
			{ApiKey: API_KEY_API_VERSIONS,
				MinVersion: 0,
				MaxVersion: 4},
			{ApiKey: API_KEY_DESCRIBE_TOPIC_PARTITIONS,
				MinVersion: 0,
				MaxVersion: 0},
		},
		ThrottleTimeMs: 0,
	}
	response.Size = response.CalculateSize()
	response.CorrelationID = correlationId
	return &response
}

func (r *ApiVersionsResponse) CalculateSize() int32 {
	return CalculateSize(*r) - 4
}

func (r ApiVersionsResponse) Serialize() []byte {
	return Serialize(r)
}
