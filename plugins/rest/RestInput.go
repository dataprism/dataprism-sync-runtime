package rest

import (
	"encoding/json"
	"errors"
	"time"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"net/http"
	"io/ioutil"
	"strconv"
	"github.com/hashicorp/golang-lru"
	"hash/crc32"
	"github.com/golang-plus/uuid"
)

type RestInputWorker struct {
	url string
	idField string
	idFieldType string
	timestampField string
	isArray bool

	interval int
	ticker *time.Ticker
	serviceName string

	tracer core.Tracer

	cache *lru.Cache
}

func NewRestInputWorker(config map[string]string, tracer core.Tracer) (core.InputWorker, error) {
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
			return nil, errors.New("the interval has to be a positive number expressing the number of milliseconds between polls")
		}

		interval = i
	}

	cache, _ := lru.New(10000)

	return &RestInputWorker{
		url: config["input_rest_url"],
		tracer: tracer,
		idField: config["input_rest_id_field"],
		idFieldType: config["input_rest_id_field_type"],
		timestampField: config["input_rest_timestamp_field"],
		isArray: isArray,
		ticker: time.NewTicker(time.Duration(interval) * time.Millisecond),
		serviceName: config["app"],
		cache: cache,
	}, nil
}

func (i *RestInputWorker) Run(done chan int, dataChannel chan core.Data) {
	for {
		select {
		case <- i.ticker.C:
			a := core.NewAction(i.serviceName, "rest-input", "FETCH")
			items, err := i.call(dataChannel)
			a.Ended(err)
			i.tracer.Action(a)

			actions := make([]*core.Action, len(items))

			for idx, v := range items {
				// -- create the action
				actions[idx] = core.NewAction(i.serviceName, "rest-input", "RECEIVE")

				// -- calculate the CRC for the data
				crc, err := calculateCRC32(v)
				if err != nil {
					actions[idx].Ended(err)
					continue
				}

				// -- check if the item is inside the cache
				if i.cache.Contains(crc) {
					i.tracer.Event(core.NewTracerEvent(i.serviceName, "rest-input", "CACHE_HIT", strconv.Itoa(int(crc))))
					actions[idx].Ended(nil)
					continue
				}

				// -- add the crc to the cache
				i.cache.Add(crc, true)
				i.tracer.Event(core.NewTracerEvent(i.serviceName, "rest-input", "CACHE_PUT", strconv.Itoa(int(crc))))

				var key string
				if i.idField == "" {
					uid, err := uuid.NewV4()

					if err != nil {
						actions[idx].Ended(err)
						continue
					}

					key = uid.String()
				} else {
					idValue := v[i.idField]

					if idValue == nil {
						actions[idx].Ended(errors.New("no id value was available on the record"))
						continue
					}

					if i.idFieldType == "number" {
						key = strconv.Itoa(int(idValue.(float64)))
					} else {
						key = idValue.(string)
					}
				}

				// -- set the id
				v["id"] = key

				jsonBytes, err := json.Marshal(v)
				if err != nil {
					actions[idx].Ended(err)
					continue
				}

				msg := &core.RawData{
					Key: []byte(key),
					Value: jsonBytes,
				}

				dataChannel <- msg

				i.tracer.Datum(NewRestTracerData(i.serviceName, "rest-input"))

				actions[idx].Ended(nil)
			}

			i.tracer.Actions(actions)

		case <- done:
			i.tracer.Event(core.NewTracerEvent(i.serviceName, "rest-input", "USER_SHUTDOWN", ""))
			i.ticker.Stop()
			break
		}
	}
}

func (i *RestInputWorker) call(dataChannel chan core.Data) ([]map[string]interface{}, error) {
	resp, err := http.DefaultClient.Get(i.url)
	if err != nil { return nil, err }

	// -- parse the response
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil { return nil, err }

	if i.isArray {
		var obj []map[string]interface{}

		err := json.Unmarshal(data, &obj)
		if err != nil { return nil, err }

		return obj, nil

	} else {
		var obj map[string]interface{}
		err := json.Unmarshal(data, &obj)
		if err != nil { return nil, err }

		return []map[string]interface{}{obj}, nil
	}


}


func calculateCRC32(obj interface{}) (uint32, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return 0, err
	}

	return crc32.ChecksumIEEE(data), nil
}

func NewRestTracerData(app string, src string) *core.TracerData {
	ts := time.Now().UTC()

	return &core.TracerData{
		Application: app,
		Timestamp: ts,
		Source: src,
		TimeDifferenceMs: 0,
	}
}