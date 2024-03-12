# tpm-kafka-common 

## what's new

### 2024-03-12
Added params to config: metadata-max-age-ms, socket-keepalive-enable, request-timeout-ms to address issues when connecting to Azure Event-Hub.
Params that are 0 or bool that are false are not set, and they keep their default values.

In the EventHub case values should be: 

- socket-keepalive-enable: true
- metadata-max-age-ms: 180000
- request-timeout-ms: 60000

### version v0.1.0
The version v0.1.0 contains significant changes from the v0.0.x revisions.

- The [TransformerProducerProcessor](tprod/processor.go) has been extended to include support for batch processing; in doing this there has been some reshuffling of params and new functionalities given to `tprod.Message`, 
signature have been modified with impact on existing processors (some fields tipically found in RequestIn are now in the Message part of it); the struct `UnimplementedTransformerProducerProcessor` can be used on existing processor
to cope with new batch methods but the signature of processMessage has been updated too

```
type echoImpl struct {
   tprod.UnimplementedTransformerProducerProcessor
   tprod.TransformerProducer
   cfg *Config
   batch []*kafka.Message
}
```

- As a side-effect of the support of batch processing, the ids of the metrics used have been changed in a number of ways. 
Now the metrics used by the infrastructure have to be put in the metrics registry and referenced in the config section as

```
ref-metrics:
  group-id: "t-prod"
```

and the id of the metrics have been changed. Below a sample of the proper ids.

```  
metrics:
t-prod:
  namespace: tpm_offload
  subsystem: consumer
  collectors:
    - id: tprod-batches
      name: tprod_batches
      help: numero batch
      type: counter
      labels:
        - id: name
          name: name
          default-value: "transform-producer"
    - id: tprod-batch-size
      name: tprod_batch_size
      help: dimensione batch
      type: gauge
      labels:
        - id: name
          name: name
          default-value: "transform-producer"
    - id: tprod-batch-errors
      name: tprod_batch_errors
      help: numero errori batch
      type: counter
      labels:
        - id: name
          name: name
          default-value: "transform-producer"
    - id: tprod-batch-duration
      name: tprod_batch_duration
      help: durata lavorazione
      type: histogram
      buckets:
        type: linear
        start: 0.5
        width-factor: 0.5
        count: 10
      labels:
        - id: name
          name: name
          default-value: "transform-producer"
    - id: tprod-event-errors
      name: tprod_msg_errors
      help: numero errori
      type: counter
      labels:
        - id: name
          name: name
          default-value: "transform-producer"
    - id: tprod-events
      name: tprod_messages
      help: numero messaggi lavorati
      type: counter
      labels:
        - id: name
          name: name
          default-value: "transform-producer"
    - id: tprod-events-to-topic
      name: tprod_messages_to_topic
      help: numero messaggi inseriti nei topic
      type: counter
      labels:
        - id: name
          name: name
          default-value: "transform-producer"
        - id: status-code
          name: status_code
          default-value: "500"
        - id: topic-name
          name: topic_name
          default-value: "N.D"
    - id: tprod-event-duration
      name: tprod_event_duration
      help: durata lavorazione
      type: histogram
      buckets:
        type: linear
        start: 0.5
        width-factor: 0.5
        count: 10
      labels:
        - id: name
          name: name
          default-value: "transform-producer"`
```

- The echo processor has been changed somewhat, and the header of the remaining attempts have been changed. It was a name very specific to the rtp project when auto-recovery from dlt
was required. That project and the use of the echo have to be fixed if an upgrade is requested.



