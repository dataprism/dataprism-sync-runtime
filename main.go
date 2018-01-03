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
	"strconv"
	"time"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
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
	dataInputChannel := make(chan core.Data)
	dataOutputChannel := make(chan []core.Data)
	errorChannel := make(chan error)
	done := make(chan int)

	if _, ok := config["buffer_duration"]; !ok { config["buffer_duration"] = "10000"; }
	if _, ok := config["buffer_size"]; !ok { config["buffer_size"] = "1000"; }
	logrus.Infof("Creating the buffer with duration %s and size %s", config["buffer_duration"], config["buffer_size"])

	size, err := strconv.Atoi(config["buffer_size"])
	if err != nil { logrus.Fatal("Unable to convert the buffer size into an integer value")}

	duration, err := strconv.Atoi(config["buffer_duration"])
	if err != nil { logrus.Fatal("Unable to convert the buffer duration into an integer value")}

	buffer := core.NewDataBuffer(dataOutputChannel, size, time.Duration(duration) * time.Second)

	logrus.Info("Starting the workers")
	go outputWorker.Run(done, dataOutputChannel, errorChannel)
	go buffer.Run(done, dataInputChannel, errorChannel)
	go inputWorker.Run(done, dataInputChannel, errorChannel)

	//select {
	//	case <- done:
	//}

	http.Handle("/metrics", promhttp.Handler())
	logrus.Fatal(http.ListenAndServe(":7654", nil))
}

func initializeMetrics() (*metrics.Metrics, error) {
	metricSink, err := prometheus.NewPrometheusSink()
	if err != nil {
		logrus.Panic("unable to create the prometheus metric sink")
	}

	return metrics.NewGlobal(metrics.DefaultConfig(*id), metricSink)
}