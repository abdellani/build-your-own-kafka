package messages

type FetchResponse struct {
	Size int32
	ResponseHeaderV1
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
	Reponses       COMPACT_ARRAY[FetchResponseReponse]
	TAG_BUFFER
}

type FetchResponseReponse struct {
	Topic      UUID
	Partitions COMPACT_ARRAY[FetchResponsePartition]
	TAG_BUFFER
}
type FetchResponsePartition struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  COMPACT_ARRAY[FectchResponseAbortedTransaction]
	PreferredReadReplica int32
	Records              COMPACT_ARRAY[int32]
	TAG_BUFFER
}

type FectchResponseAbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
	TAG_BUFFER
}
type FetchHandler struct{}

func (h *FetchHandler) Handle(r IRequest) IResponse {

	req := r.(*FetchRequest)

	res := FetchResponse{}
	res.PrepareResponse(req)

	return res
}

func (r *FetchResponse) PrepareResponse(req *FetchRequest) {
	r.CorrelationID = req.CorrelationId
	r.UpdateSize()
}
func (r *FetchResponse) UpdateSize() {
	r.Size = CalculateSize(*r) - 4
}

func (r FetchResponse) Serialize() []byte {
	return Serialize(r)
}
