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

	if val, ok := config["output_es_username"]; ok {
		username = val
	}

	if val, ok := config["output_es_password"]; ok {
		password = val
	}

	client, err := elastic.NewClient(
		elastic.SetURL(config["output_es_servers"]),
		elastic.SetBasicAuth(username, password),
	);
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	} else {
		return &ElasticSearchOutput{
			client: client,
			metrics: metricLogger,
			index: config["output_es_index"],
			kind: config["output_es_type"],
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

				_, err := o.client.Index().
					Index(o.index).
					Type("tweet").
					Id(string(dataEvent.GetKey())).
					BodyJson(string(dataEvent.GetValue())).
					Do(context.Background())

				if err != nil {
					logrus.Warn(err)
				}

			case <-errorsChannel:

			}
		}
	}()

	return nil
}


