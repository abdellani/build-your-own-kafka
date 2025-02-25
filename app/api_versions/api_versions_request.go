package api_versions

import "github.com/abdellani/build-your-own-kafka/app/types"

type APIVersionsRequest struct {
	Size int32
	types.RequestHeaderV2
	//TODO add other fields
}
