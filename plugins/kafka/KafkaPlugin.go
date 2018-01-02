package kafka

import (
	"github.com/dataprism/dataprism-sync-runtime/plugins"
)

type DataprismSyncPlugin struct {

}

func (p *DataprismSyncPlugin) Id() string { return "kafka" }

func (p *DataprismSyncPlugin) InputTypes() []*plugins.InputType {
	return []*plugins.InputType{
		{
			Id: "kafka-input",
			Factory: NewKafkaInputWorker,
			Type: "remote",
			Config: []plugins.Config{
				{"input_kafka_bootstrap_servers", "The kafka servers passed to librdkafka to connect to.", true },
				{"input_kafka_group_id", "The id of the consumer group this consumer is a member of.", true },
				{"input_kafka_topic", "The topic on the remote cluster from which the data is read.", true },
			},
		},
	}
}

func (p *DataprismSyncPlugin) OutputTypes() []*plugins.OutputType{
	return []*plugins.OutputType{
		{
			Id: "kafka-output",
			Factory: NewKafkaOutputWorker,
			Type: "remote",
			Config: []plugins.Config{
				{"input_kafka_bootstrap_servers", "The kafka servers passed to librdkafka to connect to.", true },
				{"output_kafka_data_topic", "The topic to which data will be written", true },
				{"output_kafka_error_topic", "The topic to which errors will be written. Defaults to 'errors'", false },
			},
		},
	}
}



