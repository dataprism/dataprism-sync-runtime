package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"errors"
	"github.com/dataprism/dataprism-sync-runtime/core"
	"time"
)

type KafkaInputWorker struct {
	consumer *kafka.Consumer
	topic string
	tracer core.Tracer
	serviceName string
}

func NewKafkaInputWorker(config map[string]string, tracer core.Tracer) (core.InputWorker, error) {

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
		tracer: tracer,
		consumer: consumer,
		topic: config["input.kafka.topic"],
		serviceName: config["app"],
	}

	return &res, nil
}

func (i *KafkaInputWorker) Run(done chan int, dataChannel chan core.Data) {
	a := core.NewAction(i.serviceName, "kafka-consumer", "Subscribe")
	err := i.consumer.Subscribe(i.topic, nil)
	a.Ended(err)
	i.tracer.Action(a);

	// -- return if we were unable to subscribe to the data topic
	if err != nil {
		done <- 1
	}

	run := true

	for run == true {
		select {
		case <- done:
			i.tracer.Event(core.NewTracerEvent(i.serviceName, "kafka-input", "USER_SHUTDOWN", ""))
			run = false

		case ev := <-i.consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				i.tracer.Event(NewKafkaTracerEvent(i.serviceName, "ASSIGNED_PARTITIONS", e.String()))
				a := core.NewAction(i.serviceName,"kafka-consumer", "ASSIGN_PARTITIONS")
				a.Ended(i.consumer.Assign(e.Partitions))
				i.tracer.Action(a);

			case kafka.RevokedPartitions:
				i.tracer.Event(NewKafkaTracerEvent(i.serviceName, "REVOKED_PARTITIONS", e.String()))
				a := core.NewAction(i.serviceName,"kafka-consumer", "REVOKE_PARTITIONS")
				a.Ended(i.consumer.Unassign())
				i.tracer.Action(a);

			case *kafka.Message:
				i.tracer.Datum(NewKafkaTracerData(i.serviceName, e.TopicPartition, e.Timestamp))

				dataChannel <- &core.RawData{e.Key, e.Value}

			case kafka.Error:
				i.tracer.Event(NewKafkaTracerEvent(i.serviceName, "Error", e.String()))
			}
		}
	}

	a = core.NewAction(i.serviceName, "kafka-consumer", "Close")
	a.Ended(i.consumer.Close())
	i.tracer.Action(a);
}

func NewKafkaTracerEvent(app string, kind string, payload string ) *core.TracerEvent {
	return &core.TracerEvent{
		Application: app,
		Timestamp: time.Now().UTC(),
		Component: "kafka-consumer",
		EventType: kind,
		Payload: payload,
	}
}

func NewKafkaTracerData(app string, tp kafka.TopicPartition, timestamp time.Time) *core.TracerData {
	ts := time.Now().UTC()
	src := tp.String()

	return &core.TracerData{
		Application: app,
		Timestamp: ts,
		Source: src,
		TimeDifferenceMs: int64(ts.Sub(timestamp)),
	}
}