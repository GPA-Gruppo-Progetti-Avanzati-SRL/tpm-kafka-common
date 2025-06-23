package tprod

import (
	"context"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
	"net/http"
	"sync"
)

type KafkaProducerWrapper struct {
	name               string
	producer           *kafka.Producer
	isTransactional    bool
	transactionStarted bool
	delivChan          chan kafka.Event
	mu                 *sync.Mutex
}

func NewKafkaProducerWrapper(ctx context.Context, brokerName, transactionId string, toppar kafka.TopicPartition, withDeliveryChannel bool, beginTransaction bool) (*KafkaProducerWrapper, error) {
	const semLogContext = "partitioned-message-producer::new-kafka-producer-wrapper"
	p, err := kafkalks.NewKafkaProducer(context.Background(), brokerName, transactionId, toppar)
	if err != nil {
		return nil, err
	}

	producerName := brokerName
	if toppar.Partition != kafka.PartitionAny {
		producerName = fmt.Sprintf("%s-p%d", producerName, int(toppar.Partition))
	}

	var mu *sync.Mutex
	var deliveryChannel chan kafka.Event
	if withDeliveryChannel {
		deliveryChannel = make(chan kafka.Event)
		mu = new(sync.Mutex)
	}

	kp := &KafkaProducerWrapper{name: producerName, producer: p, delivChan: deliveryChannel, mu: mu}
	if transactionId != "" {
		kp.isTransactional = true
		if beginTransaction {
			err = kp.BeginTransaction()
			if err != nil {
				return kp, err
			}
		} else {
			log.Warn().Msg(semLogContext + " - transactional producer created without starting transaction")
		}
	}
	return kp, nil
}

/*
func NewKafkaProducerWrapper(n string, p *kafka.Producer, delivChan chan kafka.Event) KafkaProducerWrapper {
	var mu *sync.Mutex
	if delivChan != nil {
		mu = new(sync.Mutex)
	}
	return KafkaProducerWrapper{name: n, producer: p, delivChan: delivChan, mu: mu}
}
*/

func (kp *KafkaProducerWrapper) Close() {
	const semLogContext = "producer-wrapper::close"
	var err error

	err = kp.AbortTransaction(context.Background())
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrState {
			// No transaction in progress, ignore the error.
			err = nil
		} else {
			LogKafkaError(err).Msg(semLogContext + " - failed to abort transaction")
		}
	}

	kp.producer.Close()
	if kp.delivChan != nil {
		close(kp.delivChan)
	}
}

func (kp *KafkaProducerWrapper) Produce(m *kafka.Message) (int, error) {
	const semLogContext = "producer-wrapper::produce"
	var err error
	st := http.StatusInternalServerError
	if kp.delivChan != nil {
		kp.mu.Lock()
		defer kp.mu.Unlock()

		err = kp.producer.Produce(m, kp.delivChan)
		if err == nil {
			e := <-kp.delivChan
			err = kp.processDeliveryEvent(e)
			if err == nil {
				st = http.StatusOK
			}
		} else {
			log.Error().Err(err).Msg(semLogContext + " - produce with synch delivery failed")
		}
	} else {
		err = kp.producer.Produce(m, nil)
		if err == nil {
			st = http.StatusAccepted
		} else {
			log.Error().Err(err).Msg(semLogContext + " - produce with a-synch delivery failed")
		}
	}

	return st, err
}

func (kp *KafkaProducerWrapper) Events() chan kafka.Event {
	return kp.producer.Events()
}

func (kp *KafkaProducerWrapper) processDeliveryEvent(evt kafka.Event) error {
	const semLogContext = "producer-wrapper::process-delivery-event"
	var err error
	switch ev := evt.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			log.Error().Err(ev.TopicPartition.Error).Int64("offset", int64(ev.TopicPartition.Offset)).Int32("partition", ev.TopicPartition.Partition).Interface("topic", ev.TopicPartition.Topic).Str(semLogTransformerProducerId, kp.name).Msg(semLogContext + " delivery failed")
			err = ev.TopicPartition.Error
		} else {
			log.Trace().Int64("offset", int64(ev.TopicPartition.Offset)).Int32("partition", ev.TopicPartition.Partition).Interface("topic", ev.TopicPartition.Topic).Str(semLogTransformerProducerId, kp.name).Msg(semLogContext + " delivery ok")
		}
	}
	return err
}

func (kp *KafkaProducerWrapper) Flush(n int) int {
	return kp.producer.Flush(n)
}

func (kp *KafkaProducerWrapper) IsInTransaction() bool {
	return kp.isTransactional && kp.transactionStarted
}

func (kp *KafkaProducerWrapper) BeginTransaction() error {
	const semLogContext = "producer-wrapper::begin-transaction"
	// Is in transaction can return false either if is not transactional or actually is not in transaction....
	if kp.isTransactional {
		if !kp.IsInTransaction() {
			log.Info().Msg(semLogContext)
			err := kp.producer.BeginTransaction()
			if err == nil {
				kp.transactionStarted = true
			}
			return err
		} else {
			log.Info().Msg(semLogContext + " - producer already in transaction")
		}
	}
	return nil
}

func (kp *KafkaProducerWrapper) AbortTransaction(ctx context.Context) error {
	const semLogContext = "producer-wrapper::abort-transaction"
	if kp.isTransactional {
		if kp.IsInTransaction() {
			log.Info().Msg(semLogContext)
			err := kp.producer.AbortTransaction(ctx)
			kp.transactionStarted = false
			if IsKafkaErrorState(err) {
				return nil
			}
			return err
		} else {
			log.Info().Msg(semLogContext + " - producer NOT in transaction")
		}
	}

	return nil
}

func (kp *KafkaProducerWrapper) CommitTransaction(ctx context.Context) error {
	const semLogContext = "producer-wrapper::commit-transaction"
	if kp.isTransactional {
		if kp.IsInTransaction() {
			log.Info().Msg(semLogContext)
			err := kp.producer.CommitTransaction(ctx)
			kp.transactionStarted = false
			return err
		} else {
			log.Info().Msg(semLogContext + " - producer NOT in transaction")
		}
	}

	return nil
}

func (kp *KafkaProducerWrapper) SendOffsetsToTransaction(ctx context.Context, offsets []kafka.TopicPartition, consumerMetadata *kafka.ConsumerGroupMetadata) error {
	const semLogContext = "producer-wrapper::send-offset-2-transaction"
	if kp.isTransactional {
		log.Info().Msg(semLogContext)
		return kp.producer.SendOffsetsToTransaction(ctx, offsets, consumerMetadata)
	}

	return nil
}

// CommitTransactionForInputPartition sends the consumer offsets for
// the given input partition and commits the current transaction.
// A new transaction will be started when done.
func (kp *KafkaProducerWrapper) CommitTransactionForInputPartition(consumer *kafka.Consumer, toppar kafka.TopicPartition) error {
	const semLogContext = "producer-wrapper::commit-tx-for-input-partition"
	position, err := consumer.Position([]kafka.TopicPartition{toppar})
	if err != nil {
		LogKafkaError(err).Msg(semLogContext)
		return err
	}

	consumerMetadata, err := consumer.GetConsumerGroupMetadata()
	if err != nil {
		LogKafkaError(err).Msg(semLogContext)
		return err
	}

	err = kp.SendOffsetsToTransaction(nil, position, consumerMetadata)
	if err != nil {
		LogKafkaError(err).Interface("position", position).Msg(semLogContext)
		err = kp.AbortTransaction(nil)
		if err != nil {
			LogKafkaError(err).Msg(semLogContext)
			return err
		}

		// Rewind this input partition to the last committed offset.
		err = kp.rewindConsumerPosition(consumer, toppar)
	} else {
		err = kp.CommitTransaction(nil)
		if err != nil {
			LogKafkaError(err).Int32("partition", toppar.Partition).Msg(semLogContext)
			abortErr := kp.AbortTransaction(nil)
			if abortErr != nil {
				LogKafkaError(err).Msg(semLogContext)
				return util.CoalesceError(abortErr, err)
			}

			// Rewind this input partition to the last committed offset.
			err = kp.rewindConsumerPosition(consumer, toppar)
		}
	}

	return err
}

// rewindConsumerPosition rewinds the consumer to the last committed offset or
// the beginning of the partition if there is no committed offset.
// This is to be used when the current transaction is aborted.
func (kp *KafkaProducerWrapper) rewindConsumerPosition(consumer *kafka.Consumer, toppar kafka.TopicPartition) error {
	const semLogContext = "producer-wrapper::rewind-consumer-position"
	committed, err := consumer.Committed([]kafka.TopicPartition{toppar}, 10*1000 /* 10s */)
	if err != nil {
		LogKafkaError(err).Msg(semLogContext)
		return err
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			// No committed offset, reset to earliest
			tp.Offset = kafka.OffsetBeginning
			tp.LeaderEpoch = nil
		}

		log.Info().Interface("partition", tp).Msg(semLogContext)

		err = consumer.Seek(tp, -1)
		if err != nil {
			LogKafkaError(err).Msg(semLogContext)
			return err
		}
	}

	return nil
}
