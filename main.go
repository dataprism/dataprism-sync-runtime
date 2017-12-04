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
	"errors"
)

func main() {

	pollInterval := 5000
	flag.IntVar(&pollInterval, "interval-ms", pollInterval, "The polling interval in milliseconds")

	signalFxToken := ""
	flag.StringVar(&signalFxToken, "signalfx", signalFxToken, "The signalFX Auth Token")

	flag.Parse()

	config := make(map[string]string, len(os.Environ()))
	for _, e := range os.Environ() {
		pair := strings.Split(e, "=")
		config[pair[0]] = pair[1]
		logrus.Info(pair[0] + " = " + pair[1])
	}

	logrus.SetLevel(logrus.DebugLevel)

	// -- initialize the metric logger
	metricLogger, err := sfx.NewSignalFXMetricLogger(signalFxToken, nil)

	// -- create the input workers
	inputWorkers, err := createInputWorkers(config, metricLogger)
	if err != nil {
		logrus.Error("Unable to create the input workers")
		logrus.Error(err)

		flag.Usage()
		os.Exit(1)
	}

	// -- create the output workers
	outputWorkers, err := createOutputWorkers(config, metricLogger)
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

func createInputWorkers(config map[string]string, metricLogger core.MetricLogger) ([]inputs.Input, error) {
	inputType, isSet := config["input_type"]
	if !isSet { return nil, errors.New("no input_type has been set") }

	inputCount := 1

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

func createOutputWorkers(config map[string]string, metricLogger core.MetricLogger) ([]outputs.Output, error) {
	outputType, isSet := config["output_type"]
	if !isSet { return nil, errors.New("no output_type has been set") }

	outputCount := 1

	result := make([]outputs.Output, outputCount)

	for i := 0; i < outputCount; i++ {
		output, err := outputs.NewOutput(outputType, config, metricLogger)

		if err != nil {
			return nil, err
		}

		result[i] = output
	}

	return result, nil
}