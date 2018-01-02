package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"errors"
	"github.com/sirupsen/logrus"
	"time"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"github.com/armon/go-metrics"
)

type KafkaOutputWorker struct {
	producer kafka.Producer
	metrics *metrics.Metrics

	errorTopic string
	dataTopic string
}

func NewKafkaOutputWorker(config map[string]string, metrics *metrics.Metrics) (core.Worker, error) {
	if _, ok := config["output_kafka_error_topic"]; !ok {
		config["output_kafka_error_topic"] = "errors"
	}

	if _, ok := config["output_kafka_data_topic"]; !ok {
		return nil, errors.New("no data topic has been set")
	}

	if _, ok := config["output_kafka_bootstrap_servers"]; !ok {
		return nil, errors.New("no bootstrap servers have been set")
	}

	c := &kafka.ConfigMap{
		"bootstrap.servers": config["output_kafka_bootstrap_servers"],
	}

	p, err := kafka.NewProducer(c)

	if err != nil {
		return nil, err
	} else {
		return &KafkaOutputWorker{
			producer: *p,
			metrics: metrics,
			errorTopic: config["output_kafka_error_topic"],
			dataTopic: config["output_kafka_data_topic"],
		}, nil
	}
}

func (o *KafkaOutputWorker) Run(done chan int, dataChannel chan core.Data, errorsChannel chan error) {
	run := true

	for run == true {
		select {
		case <-done:
			logrus.Info("Stopping Kafka Output On User Request")
			run = false

		case evt := <-o.producer.Events():
			switch ev := evt.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					logrus.Errorf("delivery failed: %v\n", m.TopicPartition.Error)
					o.metrics.IncrCounter([]string{"output.kafka.delivery_failed"}, 1)
				} else {
					logrus.Debugf("delivered message to topic %s [%d] at offset %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					o.metrics.IncrCounter([]string{"output.kafka.delivered"}, 1)
				}

			default:
				logrus.Debugf("ignored event: %s\n", ev)
			}

		case dataEvent := <-dataChannel:
			if dataEvent == nil {
				continue
			}

			o.producer.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &o.dataTopic, Partition: kafka.PartitionAny},
				Timestamp:      time.Now(),
				TimestampType:  kafka.TimestampLogAppendTime,
				Key:            dataEvent.GetKey(),
				Value:          dataEvent.GetValue(),
			}

			o.metrics.IncrCounter([]string{"output.kafka.written"}, 1)

		case errorEvent := <-errorsChannel:
			if errorEvent == nil {
				continue
			}

			o.producer.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &o.errorTopic, Partition: kafka.PartitionAny},
				Timestamp:      time.Now(),
				TimestampType:  kafka.TimestampLogAppendTime,
				Value:          []byte(errorEvent.Error()),
			}

			o.metrics.IncrCounter([]string{"output.kafka.errors"}, 1)
		}
	}
}


