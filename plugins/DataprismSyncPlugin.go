package plugins

import (
	"github.com/armon/go-metrics"
	"github.com/dataprism/dataprism-sync-runtime/core"
)

type DataprismSyncPlugin interface {
	Id() string

	InputTypes() []*InputType

	OutputTypes() []*OutputType

}

type Config struct {
	Key string
	Description string
	Required bool
}

type InputType struct {
	Id string
	Type string
	Config []Config
	Factory func(config map[string]string, metrics *metrics.Metrics) (core.InputWorker, error)
}

type OutputType struct {
	Id string
	Type string
	Config []Config
	Factory func(config map[string]string, metrics *metrics.Metrics) (core.OutputWorker, error)
}
