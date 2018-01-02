package outputs

import (
	"errors"
	"github.com/dataprism/dataprism-sync-runtime/core"
)

type Output interface {

	Start(done chan int, dataChannel chan core.Data, errorsChannel chan error) error

}

func NewOutput(outputType string, config map[string]string, metricLogger core.MetricLogger) (Output, error) {

	switch outputType {
	case "kafka":
		return NewKafkaOutput(config, metricLogger)

	case "elasticsearch":
		return NewElasticSearchOutput(config, &metricLogger)

	default:
		return nil, errors.New("Invalid output type '" + outputType + "'")
	}

}