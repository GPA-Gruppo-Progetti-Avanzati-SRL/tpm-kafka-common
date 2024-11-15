package tprod

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
	"net/http"
)

type KafkaProducerWrapper struct {
	name      string
	producer  *kafka.Producer
	delivChan chan kafka.Event
}

func NewKafkaProducerWrapper(n string, p *kafka.Producer, delivChan chan kafka.Event) KafkaProducerWrapper {
	return KafkaProducerWrapper{name: n, producer: p, delivChan: delivChan}
}

func (p KafkaProducerWrapper) Close() {
	p.producer.Close()
	if p.delivChan != nil {
		close(p.delivChan)
	}
}

func (p KafkaProducerWrapper) Produce(m *kafka.Message) (int, error) {
	const semLogContext = "producer-wrapper::produce"
	var err error
	st := http.StatusInternalServerError
	if p.delivChan != nil {
		err = p.producer.Produce(m, p.delivChan)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return st, err
		}

		e := <-p.delivChan
		err = p.processDeliveryEvent(e)
		if err == nil {
			st = http.StatusOK
		}
	} else {
		err = p.producer.Produce(m, nil)
		if err == nil {
			st = http.StatusAccepted
		}
	}

	return st, err
}

func (p KafkaProducerWrapper) processDeliveryEvent(evt kafka.Event) error {
	const semLogContext = "producer-wrapper::process-delivery-event"
	var err error
	switch ev := evt.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			log.Error().Err(ev.TopicPartition.Error).Int64("offset", int64(ev.TopicPartition.Offset)).Int32("partition", ev.TopicPartition.Partition).Interface("topic", ev.TopicPartition.Topic).Str(semLogTransformerProducerId, p.name).Msg(semLogContext + " delivery failed")
			err = ev.TopicPartition.Error
		} else {
			log.Trace().Int64("offset", int64(ev.TopicPartition.Offset)).Int32("partition", ev.TopicPartition.Partition).Interface("topic", ev.TopicPartition.Topic).Str(semLogTransformerProducerId, p.name).Msg(semLogContext + " delivery ok")
		}
	}
	return err
}

func (p KafkaProducerWrapper) Flush(n int) int {
	return p.producer.Flush(n)
}

func (kp KafkaProducerWrapper) BeginTransaction() error {
	return kp.producer.BeginTransaction()
}

func (kp KafkaProducerWrapper) AbortTransaction(ctx context.Context) error {
	return kp.producer.AbortTransaction(ctx)
}

func (kp KafkaProducerWrapper) SendOffsetsToTransaction(ctx context.Context, offsets []kafka.TopicPartition, consumerMetadata *kafka.ConsumerGroupMetadata) error {
	return kp.producer.SendOffsetsToTransaction(ctx, offsets, consumerMetadata)
}

func (kp KafkaProducerWrapper) CommitTransaction(ctx context.Context) error {
	return kp.producer.CommitTransaction(ctx)
}

func (kp KafkaProducerWrapper) Events() chan kafka.Event {
	return kp.producer.Events()
}
