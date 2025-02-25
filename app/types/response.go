package types

type IResponse interface {
	Serialize() []byte
}
