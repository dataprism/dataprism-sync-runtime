package core

type Data interface {
	GetKey() []byte

	GetValue() []byte
}
