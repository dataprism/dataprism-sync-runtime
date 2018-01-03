package rest

import (
	"encoding/json"
	"errors"
	"time"
	"github.com/sirupsen/logrus"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"net/http"
	"io/ioutil"
	"strconv"
	"github.com/armon/go-metrics"
)

type RestInputWorker struct {
	url string
	idField string
	idFieldType string
	isArray bool

	interval int
	ticker *time.Ticker

	metrics *metrics.Metrics
}

func NewRestInputWorker(config map[string]string, metrics *metrics.Metrics) (core.InputWorker, error) {
	var isArray = false
	var interval = 10000

	if _, ok := config["input_rest_url"]; !ok  {
		return nil, errors.New("no url has been provided")
	}

	if _, ok := config["input_rest_id_field"]; !ok  {
		config["input_api_id_field"] = "id"
	}

	if _, ok := config["input_rest_id_field_type"]; !ok  {
		config["input_api_id_field_type"] = "string"
	}

	if val, ok := config["input_rest_array"]; ok  {
		isArray = val == "true"
	}

	if val, ok := config["input_rest_interval"]; ok  {
		i, err := strconv.Atoi(val)

		if err != nil {
			return nil, errors.New("The interval has to be a positive number expressing the number of milliseconds between polls")
		}

		interval = i
	}

	return &RestInputWorker{
		url: config["input_rest_url"],
		metrics: metrics,
		idField: config["input_rest_id_field"],
		idFieldType: config["input_rest_id_field_type"],
		isArray: isArray,
		ticker: time.NewTicker(time.Duration(interval) * time.Millisecond),
	}, nil
}

func (i *RestInputWorker) Run(done chan int, dataChannel chan core.Data, errorsChannel chan error) {
	for {
		select {
		case <- i.ticker.C:
			logrus.Debugf("Scheduled a new request to %s", i.url)
			i.call(dataChannel, errorsChannel)

		case <- done:
			i.ticker.Stop()
			break
		}
	}
}

func (i *RestInputWorker) call(dataChannel chan core.Data, errorsChannel chan error) {
	resp, err := http.DefaultClient.Get(i.url)

	if err != nil {
		logrus.Warn("unable to make the request to " + i.url, err)
		errorsChannel <- err
	} else {
		data, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			logrus.Warn(err)
			errorsChannel <- err
		} else {
			if i.isArray {
				var obj []map[string]interface{}
				err := json.Unmarshal(data, &obj)

				if err != nil {
					logrus.Warn(err)
					errorsChannel <- err
				} else {
					for _, v := range obj {
						idValue := v[i.idField]

						if idValue == nil {
							errorsChannel <- errors.New("no id value was available on the record")
							logrus.Warn("no id value was available on the record")
							continue;
						}

						var key string
						if i.idFieldType == "number" {
							key = strconv.Itoa(int(idValue.(float64)))
						} else {
							key = idValue.(string)
						}

						jsonBytes, err := json.Marshal(v)
						if err != nil {
							logrus.Warn(err)
							errorsChannel <- err
						} else {
							msg := &core.RawData{
								Key: []byte(key),
								Value: jsonBytes,
							}

							dataChannel <- msg
						}
					}
				}
			} else {
				var obj map[string]interface{}
				err := json.Unmarshal(data, &obj)

				if err != nil {
					logrus.Warn(err)
					errorsChannel <- err
				} else {
					idValue := obj[i.idField]

					var key string
					if i.idFieldType == "number" {
						key = strconv.Itoa(int(idValue.(float64)))
					} else {
						key = idValue.(string)
					}

					msg := &core.RawData{
						Key: []byte(key),
						Value: data,
					}

					dataChannel <- msg
				}
			}
		}
	}
}


