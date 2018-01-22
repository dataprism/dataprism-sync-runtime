package core

import (
	"gopkg.in/olivere/elastic.v5"
	"errors"
	"context"
	"sync"
	"github.com/lytics/logrus"
)

type Tracer interface {
	Event(e *TracerEvent) error
	Events(e []*TracerEvent) error

	Datum(e *TracerData) error
	Data(e []*TracerData) error

	Action(a *Action) error
	Actions(a []*Action) error
}

func NewElasticsearchTracer(config map[string]string) (Tracer, error) {
	if _, ok := config["tracer_es_servers"]; !ok {
		return nil, errors.New("no elasticsearch servers have been set")
	}

	if _, ok := config["tracer_es_index"]; !ok {
		return nil, errors.New("no elasticsearch index has been set")
	}

	username := "elastic"
	password := "changeme"

	if val, ok := config["tracer_es_username"]; ok {
		username = val
	}

	if val, ok := config["tracer_es_password"]; ok {
		password = val
	}

	client, err := elastic.NewClient(
		elastic.SetURL(config["tracer_es_servers"]),
		elastic.SetBasicAuth(username, password),
		elastic.SetSniff(false),
	)

	if err != nil {
		return nil, err
	} else {
		return &ElasticsearchTracer{
			client: client,
			index: config["tracer_es_index"],
		}, nil
	}
}

type ElasticsearchTracer struct {
	Tracer

	buffer []elastic.BulkableRequest
	lock sync.Mutex

	client *elastic.Client
	index string
}

func (t *ElasticsearchTracer) Event(e *TracerEvent) error {
	return t.registerOne("event", e)
}

func (t *ElasticsearchTracer) Events(e []*TracerEvent) error {
	list := make([]elastic.BulkableRequest, len(e))

	for idx, i := range e {
		list[idx] = elastic.NewBulkIndexRequest().Type("event").Index(t.index).Doc(i)
	}

	return t.registerMany(list)
}

func (t *ElasticsearchTracer) Datum(e *TracerData) error {
	return t.registerOne("data", e)
}

func (t *ElasticsearchTracer) Data(e []*TracerData) error {
	list := make([]elastic.BulkableRequest, len(e))

	for idx, i := range e {
		list[idx] = elastic.NewBulkIndexRequest().Type("data").Index(t.index).Doc(i)
	}

	return t.registerMany(list)
}

func (t *ElasticsearchTracer) Action(a *Action) error {
	return t.registerOne("action", a)
}

func (t *ElasticsearchTracer) Actions(acts []*Action) error {
	list := make([]elastic.BulkableRequest, len(acts))

	for idx, i := range acts {
		list[idx] = elastic.NewBulkIndexRequest().Type("action").Index(t.index).Doc(i)
	}

	return t.registerMany(list)
}

func (t *ElasticsearchTracer) registerOne(kind string, doc interface{}) error {
	t.lock.Lock()
	t.buffer = append(t.buffer, elastic.NewBulkIndexRequest().Type(kind).Index(t.index).Doc(doc));
	t.lock.Unlock()

	t.rotateIfNeeded()

	return nil
}

func (t *ElasticsearchTracer) registerMany(act []elastic.BulkableRequest) error {
	t.lock.Lock()
	t.buffer = append(t.buffer, act...);
	t.lock.Unlock()

	t.rotateIfNeeded()

	return nil
}

func (t *ElasticsearchTracer) rotateIfNeeded() {
	req := t.client.Bulk()
	t.lock.Lock()
	req.Add(t.buffer...)
	t.buffer = make([]elastic.BulkableRequest, 0)
	t.lock.Unlock()

	resp, err := req.Do(context.Background())
	if err != nil {
		logrus.Error(err)
		return
	}

	logrus.Infof("flushed traces to es: %d Registered, %d failed", len(resp.Indexed()), len(resp.Failed()))
}