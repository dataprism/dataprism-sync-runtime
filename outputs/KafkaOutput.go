package outputs

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"errors"
	"github.com/sirupsen/logrus"
	"time"
	"github.com/dataprism/dataprism-sync-runtime/core"
)

type KafkaOutput struct {
	producer kafka.Producer
	metrics core.MetricLogger

	errorTopic string
	dataTopic string
}

func NewKafkaOutput(config map[string]string, metricLogger core.MetricLogger) (*KafkaOutput, error) {
	if _, ok := config["output.kafka.error_topic"]; !ok {
		config["output.kafka.error_topic"] = "errors"
	}

	if _, ok := config["output.kafka.data_topic"]; !ok {
		return nil, errors.New("no data topic has been set")
	}

	if _, ok := config["output.kafka.bootstrap_servers"]; !ok {
		return nil, errors.New("no bootstrap servers have been set")
	}

	c := &kafka.ConfigMap{
		"bootstrap.servers": config["output.kafka.bootstrap_servers"],
	}

	p, err := kafka.NewProducer(c)

	if err != nil {
		return nil, err
	} else {
		return &KafkaOutput{
			producer: *p,
			metrics: metricLogger,
			errorTopic: config["output.kafka.error_topic"],
			dataTopic: config["output.kafka.data_topic"],
		}, nil
	}
}

func (o *KafkaOutput) Start(done chan int, dataChannel chan core.Data, errorsChannel chan error) error {
	go func() {
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
						o.metrics.IncrementCounter("output.kafka.delivery_failed")
					} else {
						logrus.Debugf("delivered message to topic %s [%d] at offset %v",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
						o.metrics.IncrementCounter("output.kafka.delivered")
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

				o.metrics.IncrementCounter("output.kafka.written")

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

				o.metrics.IncrementCounter("output.kafka.errors")
			}
		}
	}()

	return nil
}


