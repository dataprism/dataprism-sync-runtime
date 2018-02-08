package s3

import (
	"github.com/dataprism/dataprism-sync-runtime/core"
	"errors"
	"strconv"
	"fmt"
	"github.com/armon/go-metrics"
	"gitlab.com/vrtoeni/dataprism-link/s3"
)

type S3Output struct {
	metrics *metrics.Metrics
	region string
	bucket string
	path string
	lineCount int
}

func NewS3Output(config map[string]string, metrics *metrics.Metrics) (core.OutputWorker, error) {
	if _, ok := config["output_s3_bucket"]; !ok {
		return nil, errors.New("no s3 bucket name has been specified")
	}

	if _, ok := config["output_s3_region"]; !ok {
		config["output_s3_region"] = "eu-west-1"
		fmt.Println("no s3 region has been set, setting to default (eu-west-1)")
	}

	if _, ok := config["output_s3_path"]; !ok {
		return nil, errors.New("no path has been specified")
	}

	if _, ok := config["output_s3_lines_per_chunk"]; !ok {
		return nil, errors.New("lines per file has not been specified")
	}


	lineCount := 6000
	if val, ok := config["output_s3_lines_per_chunk"]; !ok {
		config["output_s3_lines_per_chunk"] = strconv.Itoa(6000)
		fmt.Println("lines per file has not been specified, falling back to default value (6000)")
	} else {
		lineCount, _ = strconv.Atoi(val)
	}

	return &S3Output {
		metrics,
		config["output_s3_region"],
		config["output_s3_bucket"],
		config["output_s3_path"],
		lineCount,
	}, nil
}

func (o *S3Output) Run(done chan int, dataChannel chan []core.Data, errorsChannel chan error) {
	go func() {
		bucket, err := s3.NewBucket(o.region, o.path, o.bucket, o.lineCount, o.metrics)
		if err != nil {
			logrus.Errorf("failed to open connection to s3 bucket named %s on region %s", o.bucket, o.region)
			o.metrics.IncrementCounter("output.s3.errors")
		}
		run := true

		for run  == true {
			select {
			case <-done:
				logrus.Info("Stopping S3 Output On User Request")
				run = false

			case dataEvent := <-dataChannel:
				if dataEvent == nil {
					continue
				}

				bucket.In <- dataEvent

			case errorEvent := <- errorsChannel:
				if errorEvent == nil {
					continue
				}

				o.metrics.IncrementCounter("output.s3.errors")
			}
		}
	}()

	return nil
}