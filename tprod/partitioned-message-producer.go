package tprod

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type PartitionedMessageProducer struct {
	producers     map[int32]KafkaProducerWrapper
	isPartitioned bool
}

func NewPartitionedMessageProducer(brokerName string, transactionId string, tps kafka.TopicPartitions, withDeliveryChannel bool) (PartitionedMessageProducer, error) {
	const semLogContext = "partitioned-message-producer::new"
	mp := PartitionedMessageProducer{}
	prods := make(map[int32]KafkaProducerWrapper)
	if len(tps) == 0 {
		p, err := NewKafkaProducerWrapper(context.Background(), brokerName, transactionId, kafka.TopicPartition{Partition: kafka.PartitionAny}, withDeliveryChannel, true)
		if err != nil {
			return PartitionedMessageProducer{}, err
		}

		prods[kafka.PartitionAny] = p
	} else {
		mp.isPartitioned = true
		for _, tp := range tps {
			p, err := NewKafkaProducerWrapper(context.Background(), brokerName, transactionId, tp, withDeliveryChannel, true)
			if err != nil {
				return PartitionedMessageProducer{}, err
			}

			prods[tp.Partition] = p
		}
	}
	mp.producers = prods
	return mp, nil
}
