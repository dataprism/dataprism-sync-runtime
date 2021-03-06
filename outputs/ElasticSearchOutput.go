package outputs

import (
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"gopkg.in/olivere/elastic.v5"
	"context"
)

type ElasticSearchOutput struct {
	client *elastic.Client

	metrics *core.MetricLogger

	index string
	kind string
	pipeline string
}

func NewElasticSearchOutput(config map[string]string, metricLogger *core.MetricLogger) (*ElasticSearchOutput, error) {
	if _, ok := config["output_es_servers"]; !ok {
		return nil, errors.New("no elasticsearch servers have been set")
	}

	if _, ok := config["output_es_index"]; !ok {
		return nil, errors.New("no elasticsearch index has been set")
	}

	if _, ok := config["output_es_type"]; !ok {
		return nil, errors.New("no elasticsearch type has been set")
	}

	username := "elastic"
	password := "changeme"
	pipeline := ""

	if val, ok := config["output_es_username"]; ok {
		username = val
	}

	if val, ok := config["output_es_password"]; ok {
		password = val
	}

	if val, ok := config["output_es_pipeline"]; ok {
		pipeline = val
	}

	client, err := elastic.NewClient(
		elastic.SetURL(config["output_es_servers"]),
		elastic.SetBasicAuth(username, password),
		elastic.SetSniff(false),
	)

	if err != nil {
		return nil, err
	} else {
			return &ElasticSearchOutput{
			client: client,
			metrics: metricLogger,
			index: config["output_es_index"],
			kind: config["output_es_type"],
			pipeline: pipeline,
		}, nil
	}
}

func (o *ElasticSearchOutput) Start(done chan int, dataChannel chan core.Data, errorsChannel chan error) error {
	go func() {
		run := true

		for run == true {
			select {
			case <-done:
				logrus.Info("Stopping ElasticSearch Output On User Request")
				run = false

			case dataEvent := <-dataChannel:
				if dataEvent == nil {
					continue
				}

				req := o.client.Index().
					Index(o.index).
					Type(o.kind).
					Id(string(dataEvent.GetKey())).
					BodyJson(string(dataEvent.GetValue()));

				if len(o.pipeline) > 0 {
					req = req.Pipeline(o.pipeline)
				}
				_, err := req.Do(context.Background())

				if err != nil {
					logrus.Warn("Unable to index the events! ", err.Error())
				}

			case <-errorsChannel:

			}
		}
	}()

	return nil
}


