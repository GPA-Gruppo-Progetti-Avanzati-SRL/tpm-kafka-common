package tprod

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

func LogKafkaError(semLogContext string, err error) {
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		log.Error().Err(kErr).
			Bool("tx-requires-abort", kErr.TxnRequiresAbort()).
			Bool("timeout", kErr.IsTimeout()).
			Bool("retriable", kErr.IsRetriable()).
			Bool("fatal", kErr.IsFatal()).
			Msg(semLogContext)
	} else {
		log.Error().Err(err).Str("err-type", fmt.Sprintf("%T", kErr)).Msg(semLogContext + " - error is not kafka")
	}
}
