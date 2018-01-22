package rest

import (
	"github.com/dataprism/dataprism-sync-runtime/plugins"
)

func NewRestSyncPlugin() *plugins.DataprismSyncPlugin {
	return &plugins.DataprismSyncPlugin{
		Id: "rest",
		InputTypes: []*plugins.InputType{
			{
				Id: "rest_input",
				Type: "remote",
				Factory: NewRestInputWorker,
				Config: []plugins.Config{
					{"url", "The url from which the data is to be loaded.", true },
					{"array", "Whether or not the returned json is an array. Defaults to 'false'", false },
					{"id_field", "The name of the field containing the record id. Defaults to 'id'", false },
					{"id_field_type", "The type of the field containing the record id. Can be 'number' or 'string', but defaults to 'string'", false },
					{"interval", "A positive number expressing the number of milliseconds between polls. Defaults to 10000", false },
				},
			},
		},
		OutputTypes: []*plugins.OutputType{},
	}
}

