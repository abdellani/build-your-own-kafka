package messages

const (
	API_KEY_FETCH                     = 1
	API_KEY_API_VERSIONS              = 18
	API_KEY_DESCRIBE_TOPIC_PARTITIONS = 75
)

type SupportedVersions struct {
	MinVersion int16
	MaxVersion int16
}
