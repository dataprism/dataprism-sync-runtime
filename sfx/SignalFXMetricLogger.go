package sfx

import (
	"github.com/signalfx/golib/sfxclient"
	"github.com/sirupsen/logrus"
	"github.com/signalfx/golib/datapoint"
	"context"
	"github.com/dataprism/dataprism-sync-runtime/core"
)

type SignalFXMetricLogger struct {
	sfx *sfxclient.HTTPSink
	ready bool
	dimensions map[string]string
}

func NewSignalFXMetricLogger(token string, dimensions map[string]string) (core.MetricLogger, error) {
	if token != "" {
		result := &SignalFXMetricLogger{sfxclient.NewHTTPSink(), true, dimensions}
		result.sfx.AuthToken = token

		logrus.Info("Connected to SignalFX")
		return result, nil
	} else {
		return &SignalFXMetricLogger{ready: false, dimensions: dimensions}, nil
	}
}

func (logger *SignalFXMetricLogger) IncrementCounter(counterName string) error {
	if ! logger.ready {
		return nil
	}

	ctx := context.Background()
	return logger.sfx.AddDatapoints(ctx, []*datapoint.Datapoint{
		sfxclient.Counter(counterName, logger.dimensions, 1),
	})
}

func (logger *SignalFXMetricLogger) LogIntegerGauge(gaugeName string, value int64) error {
	if ! logger.ready {
		return nil
	}

	ctx := context.Background()
	return logger.sfx.AddDatapoints(ctx, []*datapoint.Datapoint{
		sfxclient.Gauge(gaugeName, logger.dimensions, value),
	})
}

func (logger *SignalFXMetricLogger) LogDecimalGauge(gaugeName string, value float64) error {
	if ! logger.ready {
		return nil
	}

	ctx := context.Background()
	return logger.sfx.AddDatapoints(ctx, []*datapoint.Datapoint{
		sfxclient.GaugeF(gaugeName, logger.dimensions, value),
	})

}