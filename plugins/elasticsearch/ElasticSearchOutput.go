package elasticsearch

import (
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"gopkg.in/olivere/elastic.v5"
	"context"
	"github.com/armon/go-metrics"
)

type ElasticSearchOutput struct {
	client *elastic.Client

	metrics *metrics.Metrics

	index string
	kind string
}

func NewElasticSearchOutput(config map[string]string, metrics *metrics.Metrics) (core.OutputWorker, error) {
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
		elastic.SetSniff(false),
	)

	if err != nil {
		return nil, err
	} else {
		return &ElasticSearchOutput{
			client: client,
			metrics: metrics,
			index: config["output_es_index"],
			kind: config["output_es_type"],
		}, nil
	}
}

func (o *ElasticSearchOutput) Run(done chan int, dataChannel chan []core.Data, errorsChannel chan error) {
	for {
		select {
		case <-done:
			logrus.Info("Stopping ElasticSearch Output On User Request")
			break;

		case dataEvents := <-dataChannel:
			if dataEvents == nil {
				continue
			}

			logrus.Debugf("Retrieved %d data events", len(dataEvents))

			bulk := o.client.Bulk()

			for _, e := range dataEvents {
				bulk.Add(elastic.NewBulkIndexRequest().
					Index(o.index).
					Type(o.kind).
					Id(string(e.GetKey())).
					Doc(string(e.GetValue())))
			}

			_, err := bulk.Do(context.Background())

			if err != nil {
				logrus.Warn("Unable to index the events! ", err.Error())
			}

		case err := <-errorsChannel:
			logrus.Error(err)
		}
	}
}


