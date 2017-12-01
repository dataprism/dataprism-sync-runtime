package core

type RawData struct {
	Key []byte
	Value []byte
}

func (d *RawData) GetKey() []byte {
	return d.Key
}

func (d *RawData) GetValue() []byte {
	return d.Value
}