package inputs

import (
	"errors"
	"time"
	"github.com/dataprism/dataprism-sync-runtime/core"
)

type Input interface {

	Start(ticker *time.Ticker, done chan int, dataChannel chan core.Data, errorsChannel chan error) error

}

func NewInput(inputType string, config map[string]string, metricLogger core.MetricLogger) (Input, error) {

	switch inputType {
	case "chartbeat":
		return NewChartbeatInput(config, metricLogger)

	case "api":
		return NewApiInput(config, metricLogger)

	case "kafka":
		return NewKafkaInput(config, metricLogger)

	default:
		return nil, errors.New("Invalid input type '" + inputType + "'")
	}

}