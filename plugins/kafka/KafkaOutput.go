package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"errors"
	"github.com/sirupsen/logrus"
	"time"
	"github.com/dataprism/dataprism-sync-runtime/core"
)

type KafkaOutputWorker struct {
	producer kafka.Producer
	tracer core.Tracer
	serviceName string

	errorTopic string
	dataTopic string
}

func NewKafkaOutputWorker(config map[string]string, tracer core.Tracer) (core.OutputWorker, error) {
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
			tracer: tracer,
			errorTopic: config["output_kafka_error_topic"],
			dataTopic: config["output_kafka_data_topic"],
			serviceName: config["app"],
		}, nil
	}
}

func (o *KafkaOutputWorker) Run(done chan int, dataChannel chan []core.Data) {
	run := true

	for run == true {
		select {
		case <-done:
			o.tracer.Event(core.NewTracerEvent(o.serviceName, "kafka-output", "USER_SHUTDOWN", ""))
			run = false

		case evt := <-o.producer.Events():
			switch ev := evt.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					o.tracer.Event(core.NewTracerEvent(o.serviceName, "kafka-output", "ERROR", "delivery failed: "+m.TopicPartition.Error.Error()))
				} else {
					o.tracer.Event(core.NewTracerEvent(o.serviceName, "kafka-output", "DELIVERED", "delivered "+string(m.Key)+" to "+m.TopicPartition.String()))
				}

			default:
				logrus.Debugf("ignored event: %s\n", ev)
			}

		case dataEvents := <-dataChannel:
			if dataEvents == nil {
				continue
			}

			actions := make([]*core.Action, len(dataEvents))

			for i, e := range dataEvents {
				actions[i] = core.NewAction(o.serviceName, "kafka-producer", "PRODUCE")

				actions[i].Ended(o.producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &o.dataTopic, Partition: kafka.PartitionAny},
					Timestamp:      time.Now(),
					TimestampType:  kafka.TimestampLogAppendTime,
					Key:            e.GetKey(),
					Value:          e.GetValue(),
				}, nil))
			}

			o.tracer.Actions(actions)
		default:

		}
	}
}