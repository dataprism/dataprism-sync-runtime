package main

import (
	"github.com/namsral/flag"
	"github.com/sirupsen/logrus"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"github.com/armon/go-metrics/prometheus"
	"github.com/armon/go-metrics"
	"github.com/dataprism/dataprism-sync-runtime/utils"
	"github.com/dataprism/dataprism-sync-runtime/plugins/kafka"
	"github.com/dataprism/dataprism-sync-runtime/plugins/elasticsearch"
	"github.com/dataprism/dataprism-sync-runtime/plugins/rest"
	"github.com/dataprism/dataprism-sync-runtime/plugins"
)


var (
	id = flag.String("id", "", "The application id")
)

func main() {
	flag.Parse()

	// -- read the configuration from the environment variables
	config := utils.ParseEnvVars()

	// -- initialize logging
	logrus.Info("Initializing Logging")
	logrus.SetLevel(logrus.DebugLevel)

	// -- initialize the metrics
	logrus.Info("Initializing the metrics system")
	m, err := initializeMetrics()
	if err != nil { logrus.Fatal(err) }

	// -- initialize the plugin registry
	logrus.Info("Initializing the plugin registry")
	pluginRegistry := plugins.NewPluginRegistry()
	pluginRegistry.Add(&kafka.DataprismSyncPlugin{})
	pluginRegistry.Add(&elasticsearch.DataprismSyncPlugin{})
	pluginRegistry.Add(&rest.DataprismSyncPlugin{})

	logrus.Info("Preparing the workers")
	if _, ok := config["input_type"]; !ok { logrus.Fatal("no input type has been provided") }
	inputType, inputTypeFound := pluginRegistry.GetInputType(config["input_type"])
	if !inputTypeFound { logrus.Fatalf("No input type with id %s has been found", config["input_type"])}

	if _, ok := config["output_type"]; !ok { logrus.Fatal("no output type has been provided") }
	outputType, outputTypeFound := pluginRegistry.GetOutputType(config["output_type"])
	if !outputTypeFound { logrus.Fatalf("No output plugin with id %s has been found", config["output_type"])}

	logrus.Info("Creating the workers")
	inputWorker, err := inputType.Factory(config, m)
	if err != nil { logrus.Fatal("Unable to create an instance of the input worker", err) }

	outputWorker, err := outputType.Factory(config, m)
	if err != nil { logrus.Fatal("Unable to create an instance of the output worker", err) }

	logrus.Info("Creating the data and error channels")
	dataChannel := make(chan core.Data)
	errorChannel := make(chan error)
	done := make(chan int)

	logrus.Info("Starting the workers")
	go outputWorker.Run(done, dataChannel, errorChannel)
	go inputWorker.Run(done, dataChannel, errorChannel)

	select {
		case <- done:

	}
}

func initializeMetrics() (*metrics.Metrics, error) {
	metricSink, err := prometheus.NewPrometheusSink()
	if err != nil {
		logrus.Panic("unable to create the prometheus metric sink")
	}

	return metrics.NewGlobal(metrics.DefaultConfig(*id), metricSink)
}