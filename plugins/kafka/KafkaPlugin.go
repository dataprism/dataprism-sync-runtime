package kafka

import (
	"github.com/dataprism/dataprism-sync-runtime/plugins"
)

func NewKafkaSyncPlugin() *plugins.DataprismSyncPlugin {
	return &plugins.DataprismSyncPlugin{
		Id: "kafka",
		InputTypes: []*plugins.InputType{
			{
				Id: "kafka-input",
				Factory: NewKafkaInputWorker,
				Type: "remote",
				Config: []plugins.Config{
					{"bootstrap_servers", "The kafka servers passed to librdkafka to connect to.", true },
					{"group_id", "The id of the consumer group this consumer is a member of.", true },
					{"topic", "The topic on the remote cluster from which the data is read.", true },
				},
			},
		},
		OutputTypes: []*plugins.OutputType{
			{
				Id: "kafka-output",
				Factory: NewKafkaOutputWorker,
				Type: "remote",
				Config: []plugins.Config{
					{"bootstrap_servers", "The kafka servers passed to librdkafka to connect to.", true },
					{"data_topic", "The topic to which data will be written", true },
					{"error_topic", "The topic to which errors will be written. Defaults to 'errors'", false },
				},
			},
		},
	}
}