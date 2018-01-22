package plugins

import (
	"github.com/dataprism/dataprism-sync-runtime/core"
)

type DataprismSyncPlugin struct {
	Id string
	InputTypes []*InputType
	OutputTypes []*OutputType
}

type Config struct {
	Key string				`json:"key"`
	Description string		`json:"description"`
	Required bool			`json:"required"`
}

type InputType struct {
	Id string			`json:"id"`
	Type string			`json:"type"`
	Config []Config		`json:"config"`
	Factory func(config map[string]string, tracer core.Tracer) (core.InputWorker, error)	`json:"-"`
}

type OutputType struct {
	Id string			`json:"id"`
	Type string			`json:"type"`
	Config []Config		`json:"config"`
	Factory func(config map[string]string, tracer core.Tracer) (core.OutputWorker, error)	`json:"-"`
}
