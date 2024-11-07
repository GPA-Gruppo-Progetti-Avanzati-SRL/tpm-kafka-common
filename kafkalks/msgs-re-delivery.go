package kafkalks

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

type ReDeliveryTrackInfo struct {
	MaxNumberOfRetries int
	NumberOfAttempts   int
}

func (rft *ReDeliveryTrackInfo) ShouldResend() bool {
	const semLogContext = "kafka-lks::redelivery-track-should-resend"

	if rft.MaxNumberOfRetries <= 0 {
		log.Trace().Interface("redelivery-track-info", rft).Msg(semLogContext + " no retries on failed message")
		return false
	}

	if rft.NumberOfAttempts >= rft.MaxNumberOfRetries {
		log.Warn().Interface("redelivery-track-info", rft).Msg(semLogContext + " reached max number of retries")
		return false
	}

	return true
}

type ReDeliveryOpts struct {
	maxNumberOfRetries int
}

type RedeliveryOpt func(*ReDeliveryOpts)

func WithRedeliveryMaxRetries(n int) RedeliveryOpt {
	return func(opts *ReDeliveryOpts) {
		opts.maxNumberOfRetries = n
	}
}

func ReDeliveryMessage(producer *kafka.Producer, evt *kafka.Message, opts ...RedeliveryOpt) (bool, error) {
	const semLogContext = "kafka-lks::message-re-delivery"
	var err error

	redeliveryOptions := ReDeliveryOpts{maxNumberOfRetries: 0}
	for _, o := range opts {
		o(&redeliveryOptions)
	}

	trackInfo := extractRedeliveryTrackInfoFromMessage(evt, redeliveryOptions)
	if !trackInfo.ShouldResend() {
		return false, nil
	}

	trackInfo.NumberOfAttempts++
	attemptNumberHeader := kafka.Header{Key: KafkaNumberOfDeliveryAttemptsHeaderName, Value: []byte(fmt.Sprint(trackInfo.NumberOfAttempts))}
	evt.Headers = append(evt.Headers, attemptNumberHeader)

	evt.Opaque = trackInfo
	log.Trace().Interface("event", evt).Msg(semLogContext)
	err = producer.Produce(evt, nil)
	return true, err
}

func extractRedeliveryTrackInfoFromMessage(evt *kafka.Message, defaults ReDeliveryOpts) *ReDeliveryTrackInfo {
	if evt.Opaque != nil {
		opaque, ok := evt.Opaque.(*ReDeliveryTrackInfo)
		if ok {
			return opaque
		}
	}

	return &ReDeliveryTrackInfo{MaxNumberOfRetries: defaults.maxNumberOfRetries}
}
