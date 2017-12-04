package inputs

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"errors"
	"github.com/sirupsen/logrus"
	"time"
	"github.com/dataprism/dataprism-sync-runtime/core"
)

type KafkaInputConfig struct {
	BootstrapServers string
	GroupId          string
	Topic            string
}

type KafkaInput struct {
	consumer *kafka.Consumer
	topic string
	metrics core.MetricLogger
}

func NewKafkaInput(config map[string]string, metricLogger core.MetricLogger) (*KafkaInput, error) {

	if _, ok := config["input_kafka_topic"]; !ok {
		return nil, errors.New("no data topic has been set")
	}

	if _, ok := config["input_kafka_group_id"]; !ok {
		return nil, errors.New("no group id has been set")
	}

	if _, ok := config["input_kafka_bootstrap_servers"]; !ok {
		return nil, errors.New("no bootstrap servers have been set")
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": 				config["input_kafka_bootstrap_servers"],
		"group.id":                        	config["input_kafka_group_id"],
		"session.timeout.ms":              	6000,
		"go.events.channel.enable":        	true,
		"go.application.rebalance.enable": 	true,
		"default.topic.config":            	kafka.ConfigMap{"auto.offset.reset": "earliest"},
	})

	if err != nil {
		return nil, err
	}

	return &KafkaInput{
		metrics: metricLogger,
		consumer: consumer,
		topic: config["input.kafka.topic"],
	}, nil
}

func (i *KafkaInput) Start(ticker *time.Ticker, done chan int, dataChannel chan core.Data, errorsChannel chan error) error {
	err := i.consumer.Subscribe(i.topic, nil)

	// -- return if we were unable to subscribe to the data topic
	if err != nil {
		return err
	}

	logrus.Infof("Subscribed to topic %s", i.topic)

		run := true

		for run == true {
			select {
			case <- done:
				logrus.Info("Stopping Kafka Input On User Request")
				run = false

			case ev := <-i.consumer.Events():
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					i.metrics.IncrementCounter("input.kafka.assigned_partitions")
					i.consumer.Assign(e.Partitions)

				case kafka.RevokedPartitions:
					i.metrics.IncrementCounter("input.kafka.revoked_partitions")
					i.consumer.Unassign()

				case *kafka.Message:
					dataChannel <- &core.RawData{e.Key, e.Value}
					i.metrics.IncrementCounter("input.kafka.read")

				case kafka.PartitionEOF:
					i.metrics.IncrementCounter("input.kafka.partition_eof")

				case kafka.Error:
					errorsChannel <- e
					logrus.Error(e)
					i.metrics.IncrementCounter("input.kafka.errors")
					run = false
				}
			}
		}

		logrus.Info("Closing Consumer")
		i.consumer.Close()

	return nil
}
