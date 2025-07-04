package tprod

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func LogKafkaError(err error) *zerolog.Event {
	var kErr kafka.Error
	var evt *zerolog.Event

	if err == nil {
		evt = log.Trace()
		return evt
	}

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

func IsKafkaError(err error) bool {
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		return true
	}

	return false
}

func KafkaErrorRequiresAbort(err error, OnNonKafkaErrors bool) bool {
	if err == nil {
		return false
	}

	var kErr kafka.Error
	if errors.As(err, &kErr) {
		// return kErr.TxnRequiresAbort()
		return true
	} else {
		return OnNonKafkaErrors
	}

}

func IsKafkaErrorState(err error) bool {
	if err == nil {
		return false
	}

	var kErr kafka.Error
	if errors.As(err, &kErr) {
		if kErr.Code() == kafka.ErrState {
			return true
		}
	}

	return false
}

func isKafkaErrorFatal(err error) bool {
	if err == nil {
		return false
	}

	var kErr kafka.Error
	if errors.As(err, &kErr) {
		return kErr.IsFatal()
	}

	return false
}
