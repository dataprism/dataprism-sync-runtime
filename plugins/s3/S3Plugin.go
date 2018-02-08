package s3

import "github.com/dataprism/dataprism-sync-runtime/plugins"

type DataprismSyncPlugin struct {

}

func (p *DataprismSyncPlugin) Id() string { return "s3" }

func (p *DataprismSyncPlugin) InputTypes() []*plugins.InputType {
	return []*plugins.InputType{

	}
}

func (p *DataprismSyncPlugin) OutputTypes() []*plugins.OutputType {
	return []*plugins.OutputType {
		{
			Id: "s3_output",
			Type: "remote",
			Factory: NewS3Output,
			Config: []plugins.Config {
				{ "output_s3_bucket", "the S3 bucket name", true },
				{ "output_s3_region", "AWS region where the bucket is located", false },
				{ "output_s3_lines_per_chunk", "number of entries per written file", false },
			},
		},
	}
}