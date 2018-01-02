# Dataprism Sync Runtime
This projects holds the process which is running inside the docker containers spawned by dataprism sync.

The runtime is built around plugins and makes heavy use of go's channels to ship data from inputs to outputs
. No actual transformation is being done on the data, it is just taken from the input and sent to the output. 
Processing should be done before or after syncing.

The code uses the notion of plugins, but a refactoring towards golang plugins still has to be done and is part of 
the roadmap.

A Sync Runtime process always combines a single inputType with a single outputType. Multiple processes need to be
spawned if more input/outputs are needed.

Configuration is done through environment variables. Take a look at the plugins (KafkaPlugin, RestPlugin, ...) to get 
an idea of what can be configured.

## Plugins
The following plugins are currently available and provide inputs and/or outputs.

### Apache Kafka
The kafka plugin includes a KafkaInput and a KafkaOutput to read data from a topic inside a kafka cluster or
write data to a topic in a kafka cluster. It makes use of the widely used librdkafka library.

```
brew install pkg-config
brew install librdkafka
```

| kafka     | |
| --------- | ---------------------------------------------- |
| plugin    | [KafkaPlugin](../blob/master/plugins/kafka/KafkaPlugin) |
| inputs    | [KafkaInput](../blob/master/plugins/kafka/KafkaInput) |
| outputs   | [KafkaOutput](../blob/master/plugins/kafka/KafkaOutput) |

### ElasticSearch
The ElasticSearch plugin contains an output which sends events to elasticsearch to be indexed.

| elasticsearch     | |
| ----------------- | ---------------------------------------------- |
| plugin            | [ElasticSearchPlugin](../blob/master/plugins/elasticsearch/ElasticSearchPlugin) |
| inputs            |  |
| outputs           | [ElasticSearchOutput](../blob/master/plugins/elasticsearch/ElasticSearchOutput) |

### Rest
The rest plugin provides an input to read from a rest API. It does so in a pretty naive way by just requesting
a response on a pre-configured interval.

| rest      | |
| --------- | ---------------------------------------------- |
| plugin    | [RestPlugin](../blob/master/plugins/rest/RestPlugin) |
| inputs    | [RestInput](../blob/master/plugins/rest/RestInput) |
| outputs   | |
