package rest

import (
	"github.com/dataprism/dataprism-sync-runtime/plugins"
)

type DataprismSyncPlugin struct {

}

func (p *DataprismSyncPlugin) Id() string { return "rest" }

func (p *DataprismSyncPlugin) InputTypes() []*plugins.InputType {
	return []*plugins.InputType{
		{
			Id: "rest_input",
			Type: "remote",
			Factory: NewRestInputWorker,
			Config: []plugins.Config{
				{"input_rest_url", "The url from which the data is to be loaded.", true },
				{"input_rest_array", "Whether or not the returned json is an array. Defaults to 'false'", false },
				{"input_rest_id_field", "The name of the field containing the record id. Defaults to 'id'", false },
				{"input_rest_id_field_type", "The type of the field containing the record id. Can be 'number' or 'string', but defaults to 'string'", false },
				{"input_rest_interval", "A positive number expressing the number of milliseconds between polls. Defaults to 10000", false },
			},
		},
	}
}

func (p *DataprismSyncPlugin) OutputTypes() []*plugins.OutputType{
	return []*plugins.OutputType{

	}
}



