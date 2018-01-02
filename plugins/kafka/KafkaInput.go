package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"github.com/armon/go-metrics"
)

type KafkaInputWorker struct {
	consumer *kafka.Consumer
	topic string
	metrics *metrics.Metrics
}

func NewKafkaInputWorker(config map[string]string, metrics *metrics.Metrics) (core.Worker, error) {

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

	res := KafkaInputWorker{
		metrics: metrics,
		consumer: consumer,
		topic: config["input.kafka.topic"],
	}

	return &res, nil
}

func (i *KafkaInputWorker) Run(done chan int, dataChannel chan core.Data, errorsChannel chan error) {
	err := i.consumer.Subscribe(i.topic, nil)

	// -- return if we were unable to subscribe to the data topic
	if err != nil {
		errorsChannel <- err
		done <- 1
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
				i.metrics.IncrCounter([]string{"input.kafka.assigned_partitions"}, 1)
				i.consumer.Assign(e.Partitions)

			case kafka.RevokedPartitions:
				i.metrics.IncrCounter([]string{"input.kafka.revoked_partitions"}, 1)
				i.consumer.Unassign()

			case *kafka.Message:
				dataChannel <- &core.RawData{e.Key, e.Value}
				i.metrics.IncrCounter([]string{"input.kafka.read"}, 1)

			case kafka.PartitionEOF:
				i.metrics.IncrCounter([]string{"input.kafka.partition_eof"}, 1)

			case kafka.Error:
				errorsChannel <- e
				logrus.Error(e)
				i.metrics.IncrCounter([]string{"input.kafka.errors"}, 1)
				run = false
			}
		}
	}

	logrus.Info("Closing Consumer")
	i.consumer.Close()
}
