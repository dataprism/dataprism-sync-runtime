# Dataprism Sync Runtime

## Requirements
```
brew install pkg-config
brew install librdkafka
```

## Inputs
### API
Api input can poll an api for data at regular intervals.

| Environment Var           | Description                                    | Required | Default  |
| ------------------------- | ---------------------------------------------- | :------: | -------- |
| input_api_url             | The url to poll                                | yes      |          |
| input_api_id_field        | The name of the field holding the unique id    | no       | "id"     |
| input_api_id_field_type   | The type of the field holding the unique id. "string" and "number" are supported | no       | "string" |
| input_api_array           | Is the response from the url a json array?     | no       | yes      |

### Kafka
The Kafka Input can listen for data on specific topics on a given kafka cluster. 

| Environment Var                 | Description                                    | Required | Default          |
| ------------------------------- | ---------------------------------------------- | :------: | ---------------- |
| input_kafka_bootstrap_servers   | The kafka nodes to connect to                  | yes      | |
| input_kafka_group_id            | The name of the group to which the listener belongs | yes |     |
| input_kafka_topic               | The topic from which to read the data          | yes      |          |

## Outputs
### ElasticSearch
### Kafka
