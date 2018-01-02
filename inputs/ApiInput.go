package inputs

import (
	"encoding/json"
	"errors"
	"time"
	"github.com/sirupsen/logrus"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"net/http"
	"io/ioutil"
)

type ApiInput struct {
	url string
	idField string

	isArray bool
	interval int

	metrics core.MetricLogger
}

func NewApiInput(config map[string]string, metricLogger core.MetricLogger) (*ApiInput, error) {
	var url string
	var idField string = "id"
	var isArray = false

	if val, ok := config["input_api_url"]; ok  {
		if !ok {
			return nil, errors.New("no url has been provided")
		}

		url = val
	}

	if val, ok := config["input_api_id_field"]; ok  {
		if !ok {
			return nil, errors.New("no id_field has been provided")
		}

		url = val
	}

	if val, ok := config["input_api_array"]; ok  {
		if ok {
			isArray = val == "true"
		}
	}

	return &ApiInput{
		url: url,
		metrics: metricLogger,
		idField: idField,
		isArray: isArray,
	}, nil
}

func (i *ApiInput) Start(ticker *time.Ticker, done chan int, dataChannel chan core.Data, errorsChannel chan error) error {
	for {
		select {
		case <- ticker.C:
			logrus.Debug("Scheduled a new request to chartbeat")
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
		logrus.Warn(err)
		errorsChannel <- err
	} else {
		data, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			logrus.Warn(err)
			errorsChannel <- err
		} else {
			if i.isArray {
				var obj []map[string]interface{}
				err := json.Unmarshal(data, obj)

				if err != nil {
					logrus.Warn(err)
					errorsChannel <- err
				} else {
					for _, v := range obj {
						msg := &core.RawData{
							Key: []byte(v[i.idField].(string)),
							Value: data,
						}

						dataChannel <- msg
					}
				}
			} else {
				var obj map[string]interface{}
				err := json.Unmarshal(data, obj)

				if err != nil {
					logrus.Warn(err)
					errorsChannel <- err
				} else {
					msg := &core.RawData{
						Key: []byte(obj[i.idField].(string)),
						Value: data,
					}

					dataChannel <- msg
				}
			}
		}
	}
}


