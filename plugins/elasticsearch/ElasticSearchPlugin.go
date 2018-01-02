package elasticsearch

import (
	"github.com/dataprism/dataprism-sync-runtime/plugins"
)

type DataprismSyncPlugin struct {

}

func (p *DataprismSyncPlugin) Id() string { return "elasticsearch" }

func (p *DataprismSyncPlugin) InputTypes() []*plugins.InputType {
	return []*plugins.InputType{

	}
}

func (p *DataprismSyncPlugin) OutputTypes() []*plugins.OutputType{
	return []*plugins.OutputType{
		{
			Id: "elasticsearch_output",
			Type: "remote",
			Factory: NewElasticSearchOutput,
			Config: []plugins.Config{
				{"output_es_servers", "The url of the elasticsearch server", true },
				{"output_es_index", "The index under which new entries will be stored", true },
				{"output_es_type", "The type under which new entries will be stored", true },
				{"output_es_username", "The elasticsearch username, defaults to 'elastic'", false },
				{"output_es_password", "The elasticsearch password, defaults to 'changeme'", false },
			},
		},
	}
}



