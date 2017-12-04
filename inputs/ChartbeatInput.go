package inputs

import (
	"github.com/golang-plus/uuid"
	"encoding/json"
	"hash/crc32"
	"errors"
	"time"
	"github.com/sirupsen/logrus"
	"github.com/hashicorp/golang-lru"
	"strconv"
	"github.com/dataprism/dataprism-sync-runtime/chartbeat"
	"github.com/dataprism/dataprism-sync-runtime/core"
)

type ChartbeatInput struct {
	limit int

	client chartbeat.ChartbeatLive
	cache *lru.Cache

	metrics core.MetricLogger
}

func NewChartbeatInput(config map[string]string, metricLogger core.MetricLogger) (*ChartbeatInput, error) {
	limit := 10000
	cacheSize := 2 * limit

	if val, ok := config["input_chartbeat_limit"]; ok {
		l, err := strconv.Atoi(val)
		if err == nil {
			logrus.Infof("setting chartbeat input request limit to %d", l)
			limit = l
		} else {
			logrus.Warn("falling back to default chartbeat input request limit of 10000")
		}
	}

	if val, ok := config["input_chartbeat_cache_size"]; ok  {
		l, err := strconv.Atoi(val)
		if err == nil {
			logrus.Infof("setting chartbeat input cache to %d", l)
			cacheSize = l
		} else {
			logrus.Warnf("falling back to default chartbeat input cache of %d", cacheSize)
		}
	}

	cache, _ := lru.New(cacheSize)
	logrus.Infof("Created the ChartBeat Visitor Cache with room for %d items", cacheSize)

	return &ChartbeatInput{
		cache: cache,
		metrics: metricLogger,
		client: chartbeat.ChartbeatLive{
			Config: &chartbeat.ChartbeatLiveConfig{
				Domain: config["input_chartbeat_domain"],
				ApiKey: config["input_chartbeat_api_key"],
				Limit: limit,
			},
		},
	}, nil
}

func (i *ChartbeatInput) Start(ticker *time.Ticker, done chan int, dataChannel chan core.Data, errorsChannel chan error) error {
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

func (i *ChartbeatInput) call(dataChannel chan core.Data, errorsChannel chan error) {
	visitors, err := i.client.RecentVisitors()
	logrus.Infof("A new batch of %d records has been received from ChartBeat", len(visitors))

	if err != nil {
		logrus.Warn(err)
		errorsChannel <- err
	} else {
		hits := 0

		// -- convert the data we received into maps
		for j := range visitors {
			// -- calculate the CRC for the data
			crc, err := calculateCRC32(visitors[j])

			if err != nil {
				logrus.Warn(err)
				errorsChannel <- errors.New("unable to calculate the CRC32 for the received visitor")
			} else {
				// -- check if the crc is already in the cache
				if !i.cache.Contains(crc) {
					id, err := uuid.NewV4()

					if err != nil {
						logrus.Warn(err)
						errorsChannel <- err
						continue

					} else {
						i.cache.Add(crc, id.String())
					}
				} else {
					hits++
				}

				value, err := json.Marshal(visitors[j])
				if err != nil {
					logrus.Warn(err)
					errorsChannel <- err
					continue
				}

				key, _ := i.cache.Get(crc)

				msg := &core.RawData{
					Key: []byte(key.(string)),
					Value: value,
				}

				dataChannel <- msg
			}
		}

		ratio := (float64(hits) / float64(len(visitors))) * 100

		logrus.Infof("Overlap ratio: %f%%, %d visitors received", ratio, len(visitors))

		i.metrics.LogIntegerGauge("input.chartbeat.read", int64(len(visitors)))
		i.metrics.LogIntegerGauge("input.chartbeat.cache_hits", int64(hits))
		i.metrics.LogDecimalGauge("input.chartbeat.overlap", ratio)
	}
}

func calculateCRC32(obj interface{}) (uint32, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return 0, err
	}

	return crc32.ChecksumIEEE(data), nil
}


