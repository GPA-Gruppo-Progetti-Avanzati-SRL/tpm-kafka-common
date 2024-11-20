package tprod

import (
	"context"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
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
		p, err := newKafkaProducerWrapper(context.Background(), brokerName, transactionId, kafka.TopicPartition{Partition: kafka.PartitionAny}, withDeliveryChannel)
		if err != nil {
			return PartitionedMessageProducer{}, err
		}

		prods[kafka.PartitionAny] = p
	} else {
		mp.isPartitioned = true
		for _, tp := range tps {
			p, err := newKafkaProducerWrapper(context.Background(), brokerName, transactionId, tp, withDeliveryChannel)
			if err != nil {
				return PartitionedMessageProducer{}, err
			}

			prods[tp.Partition] = p
		}
	}
	mp.producers = prods
	return mp, nil
}

func newKafkaProducerWrapper(ctx context.Context, brokerName, transactionId string, toppar kafka.TopicPartition, withDeliveryChannel bool) (KafkaProducerWrapper, error) {
	const semLogContext = "partitioned-message-producer::new-kafka-producer-wrapper"
	p, err := kafkalks.NewKafkaProducer(context.Background(), brokerName, transactionId, toppar)
	if err != nil {
		return KafkaProducerWrapper{}, err
	}

	producerName := brokerName
	if toppar.Partition != kafka.PartitionAny {
		producerName = fmt.Sprintf("%s-p%d", producerName, int(toppar.Partition))
	}

	var deliveryChannel chan kafka.Event
	if withDeliveryChannel {
		deliveryChannel = make(chan kafka.Event)
	}

	kp := NewKafkaProducerWrapper(producerName, p, deliveryChannel)
	return kp, nil
}
