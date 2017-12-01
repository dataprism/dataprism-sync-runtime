package core

type MetricLogger interface {

	IncrementCounter(counterName string) error

	LogIntegerGauge(gaugeName string, value int64) error

	LogDecimalGauge(gaugeName string, value float64) error

}
