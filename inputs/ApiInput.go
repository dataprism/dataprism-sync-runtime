package inputs

import (
	"encoding/json"
	"errors"
	"time"
	"github.com/sirupsen/logrus"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"net/http"
	"io/ioutil"
	"strconv"
)

type ApiInput struct {
	url string
	idField string
	idFieldType string

	isArray bool
	interval int

	metrics core.MetricLogger
}

func NewApiInput(config map[string]string, metricLogger core.MetricLogger) (*ApiInput, error) {
	var url string
	var idField = "id"
	var idFieldType = "string"
	var isArray = false

	if val, ok := config["input_api_url"]; ok  {
		url = val
	} else {
		return nil, errors.New("no url has been provided")
	}

	if val, ok := config["input_api_id_field"]; ok  {
		idField = val
	} else {
		return nil, errors.New("no id_field has been provided")
	}

	if val, ok := config["input_api_id_field_type"]; ok  {
		idFieldType = val
	}

	if val, ok := config["input_api_array"]; ok  {
		isArray = val == "true"
	}

	return &ApiInput{
		url: url,
		metrics: metricLogger,
		idField: idField,
		idFieldType: idFieldType,
		isArray: isArray,
	}, nil
}

func (i *ApiInput) Start(ticker *time.Ticker, done chan int, dataChannel chan core.Data, errorsChannel chan error) error {
	for {
		select {
		case <- ticker.C:
			logrus.Debugf("Scheduled a new request to %s", i.url)
			i.call(dataChannel, errorsChannel)

		case <- done:
			ticker.Stop()
			return nil
		}
	}
}

func (i *ApiInput) call(dataChannel chan core.Data, errorsChannel chan error) {
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


