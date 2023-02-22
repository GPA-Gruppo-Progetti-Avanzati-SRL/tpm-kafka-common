package kafkalks

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

type LinkedServices []*LinkedService

var theRegistry LinkedServices

func Initialize(cfgs []Config) (LinkedServices, error) {

	const semLogContext = "kafka linked-services initialization"
	if len(cfgs) == 0 {
		log.Info().Msg(semLogContext + ": no config provided....skipping")
		return nil, nil
	}

	if len(theRegistry) != 0 {
		log.Warn().Msg(semLogContext + ": registry already configured.. overwriting")
	}

	log.Info().Int("no-linked-services", len(cfgs)).Msg(semLogContext)

	var r LinkedServices
	for _, kcfg := range cfgs {
		lks, err := NewKafkaServiceInstanceWithConfig(kcfg)
		if err != nil {
			return nil, err
		}

		r = append(r, lks)
		log.Info().Msg("kafka instance configured")

	}

	theRegistry = r
	return r, nil
}

func GetKafkaLinkedService(brokerName string) (*LinkedService, error) {

	const semLogContext = "get kafka linked-service"

	log.Trace().Str("broker", brokerName).Msg(semLogContext)

	for _, k := range theRegistry {
		if k.Name() == brokerName {
			return k, nil
		}
	}

	err := errors.New("kafka linked service not found by name " + brokerName)
	log.Error().Err(err).Str("broker-name", brokerName).Msg(semLogContext)
	return nil, err
}

func NewKafkaConsumer(brokerName, gId string, autoCommit bool) (*kafka.Consumer, error) {
	k, err := GetKafkaLinkedService(brokerName)
	if err != nil {
		return nil, err
	}

	return k.NewConsumer(gId, autoCommit)
}

func NewKafkaProducer(ctx context.Context, brokerName, tId string) (*kafka.Producer, error) {
	k, err := GetKafkaLinkedService(brokerName)
	if err != nil {
		return nil, err
	}

	return k.NewProducer(ctx, tId)
}
