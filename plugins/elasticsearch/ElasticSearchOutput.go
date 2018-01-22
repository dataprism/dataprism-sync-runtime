package elasticsearch

import (
	"errors"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"gopkg.in/olivere/elastic.v5"
	"context"
)

type ElasticSearchOutput struct {
	client *elastic.Client

	tracer core.Tracer
	serviceName string

	index string
	kind string
}

func NewElasticSearchOutput(config map[string]string, tracer core.Tracer) (core.OutputWorker, error) {
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
			tracer: tracer,
			serviceName: config["app"],
			index: config["output_es_index"],
			kind: config["output_es_type"],
		}, nil
	}
}

func (o *ElasticSearchOutput) Run(done chan int, dataChannel chan []core.Data) {
	for {
		select {
		case <-done:
			o.tracer.Event(core.NewTracerEvent(o.serviceName, "elasticsearch-output", "USER_SHUTDOWN", ""))
			break

		case dataEvents := <-dataChannel:
			if dataEvents == nil {
				break
			}

			bulk := o.client.Bulk()

			actions := make([]*core.Action, len(dataEvents))

			for idx, e := range dataEvents {
				bulk.Add(elastic.NewBulkIndexRequest().
					Index(o.index).
					Type(o.kind).
					Id(string(e.GetKey())).
					Doc(string(e.GetValue())))

				actions[idx] = core.NewAction(o.serviceName, "elasticsearch-output", "INDEX")
			}

			resp, err := bulk.Do(context.Background())

			// -- end all actions if something went wrong sending to ES
			if err != nil {
				for _, v := range actions {
					v.Ended(err)
				}
				break
			}

			for idx, v := range resp.Items {
				r, ok := v["index"]
				if !ok {
					continue
				}

				if r.Status >= 200 && r.Status < 300 {
					actions[idx].Ended(nil)
				} else {
					actions[idx].Ended(errors.New(r.Error.Reason))
				}
			}

			o.tracer.Actions(actions)
		default:

		}
	}
}