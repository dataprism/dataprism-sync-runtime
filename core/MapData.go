package core

type MapData struct {
	Key string
	Value map[string]interface{}
}

func (m *MapData) GetKey() string {
	return m.Key
}

func (m *MapData) GetValue() map[string]interface{} {
	return m.Value
}