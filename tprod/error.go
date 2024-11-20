package tprod

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func logKafkaError(err error) *zerolog.Event {
	var kErr kafka.Error
	var evt *zerolog.Event
	if errors.As(err, &kErr) {
		evt = log.Error().Err(kErr).
			Bool("tx-requires-abort", kErr.TxnRequiresAbort()).
			Bool("timeout", kErr.IsTimeout()).
			Bool("retriable", kErr.IsRetriable()).
			Bool("fatal", kErr.IsFatal()).
			Int("code", int(kErr.Code()))
	} else {
		evt = log.Error().Err(err).Str("err-type", fmt.Sprintf("%T", kErr))
	}

	return evt
}
