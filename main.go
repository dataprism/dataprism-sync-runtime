package main

import (
	"time"
	"github.com/namsral/flag"
	"os"
	"github.com/sirupsen/logrus"
	"strings"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"github.com/dataprism/dataprism-sync-runtime/inputs"
	"github.com/dataprism/dataprism-sync-runtime/outputs"
	"github.com/dataprism/dataprism-sync-runtime/sfx"
)

func main() {

	pollInterval := 5000
	flag.IntVar(&pollInterval, "interval-ms", pollInterval, "The polling interval in milliseconds")

	inputCount := 1
	flag.IntVar(&inputCount, "input-workers", inputCount, "Input Worker Count")

	inputType := ""
	flag.StringVar(&inputType, "input-type", inputType, "The Type of input to use")

	outputCount := 1
	flag.IntVar(&outputCount, "output-workers", outputCount, "Collector Worker Count")

	outputType := ""
	flag.StringVar(&outputType, "output-type", outputType, "The Type of collector to use")

	signalFxToken := ""
	flag.StringVar(&signalFxToken, "signalfx", signalFxToken, "The signalFX Auth Token")

	flag.Parse()

	config := make(map[string]string, len(os.Environ()))
	for _, e := range os.Environ() {
		pair := strings.Split(e, "=")
		config[pair[0]] = pair[1]
	}

	logrus.SetLevel(logrus.DebugLevel)

	// -- initialize the metric logger
	metricLogger, err := sfx.NewSignalFXMetricLogger(signalFxToken, nil)

	// -- create the input workers
	inputWorkers, err := createInputWorkers(inputType, config, inputCount, metricLogger)
	if err != nil {
		logrus.Error("Unable to create the input workers")
		logrus.Error(err)

		flag.Usage()
		os.Exit(1)
	}

	// -- create the output workers
	outputWorkers, err := createOutputWorkers(outputType, config, outputCount, metricLogger)
	if err != nil {
		logrus.Error("Unable to create the output workers")
		logrus.Error(err)

		flag.Usage()
		os.Exit(1)
	}

	// -- Start the flow

	//requests := make(chan core.PollRequest)
	//defer close(requests)

	dataChannel := make(chan core.Data)
	//defer close(messages)

	errorChannel := make(chan error)
	//defer close(errors)

	done := make(chan int)

	for i := range outputWorkers {
		outputWorkers[i].Start(done, dataChannel, errorChannel)

		logrus.Info("Started a new Collector")
	}

	ticker := time.NewTicker(time.Duration(pollInterval) * time.Millisecond)
	for i := range inputWorkers {
		inputWorkers[i].Start(ticker, done, dataChannel, errorChannel)

		logrus.Info("Started a new input")
	}
}

func createInputWorkers(inputType string, config map[string]string, inputCount int, metricLogger core.MetricLogger) ([]inputs.Input, error) {
	result := make([]inputs.Input, inputCount)

	for i := 0; i < inputCount; i++ {
		input, err := inputs.NewInput(inputType, config, metricLogger)

		if err != nil {
			return nil, err
		}

		result[i] = input
	}

	return result, nil
}

func createOutputWorkers(collectorType string, config map[string]string, outputCount int, metricLogger core.MetricLogger) ([]outputs.Output, error) {
	result := make([]outputs.Output, outputCount)

	for i := 0; i < outputCount; i++ {
		output, err := outputs.NewOutput(collectorType, config, metricLogger)

		if err != nil {
			return nil, err
		}

		result[i] = output
	}

	return result, nil
}