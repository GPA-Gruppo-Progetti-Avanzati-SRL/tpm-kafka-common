package tprod

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

func rewindConsumer(consumer *kafka.Consumer) error {
	const semLogContext = "t-prod::rewind-consumer"

	partitions, err := consumer.Assignment()
	if err != nil {
		LogKafkaError(err).Msg(semLogContext + " consumer assignment error")
		return nil
	}

	if len(partitions) == 0 {
		log.Info().Msg(semLogContext + " no assignment to rewind")
		return nil
	}

	committed, err := consumer.Committed(partitions, 10*1000)
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

		log.Info().Int32("partition", tp.Partition).Int64("offset", int64(tp.Offset)).Msg(semLogContext + " rewinding input partition")
	}

	_, err = consumer.SeekPartitions(committed)
	if err != nil {
		LogKafkaError(err).Msg(semLogContext + " seek partitions error")
		return err
	}

	return nil
}
