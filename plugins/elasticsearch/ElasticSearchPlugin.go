package elasticsearch

import (
	"github.com/dataprism/dataprism-sync-runtime/plugins"
)

func NewElasticsearchSyncPlugin() *plugins.DataprismSyncPlugin {
	return &plugins.DataprismSyncPlugin{
		Id: "elasticsearch",
		InputTypes: []*plugins.InputType{},
		OutputTypes: []*plugins.OutputType{
			{
				Id: "elasticsearch_output",
				Type: "remote",
				Factory: NewElasticSearchOutput,
				Config: []plugins.Config{
					{"servers", "The url of the elasticsearch server", true },
					{"index", "The index under which new entries will be stored", true },
					{"type", "The type under which new entries will be stored", true },
					{"username", "The elasticsearch username, defaults to 'elastic'", false },
					{"password", "The elasticsearch password, defaults to 'changeme'", false },
				},
			},
		},
	}
}



