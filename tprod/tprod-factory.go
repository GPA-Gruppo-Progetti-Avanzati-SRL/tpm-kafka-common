package tprod

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
	"sync"

	"strings"
	"time"
)

func NewTransformerProducer(cfg *TransformerProducerConfig, wg *sync.WaitGroup, processor TransformerProducerProcessor) (TransformerProducer, error) {
	const semLogContext = "t-prod-factory::new"
	var err error

	if cfg.WorkMode != WorkModeBatch {
		cfg.WorkMode = WorkModeMsg
	}

	// Backward compatibility for vintage errors.
	if cfg.OnError != "" {
		log.Warn().Msg(semLogContext + " - Deprecated OnError property set")
		if len(cfg.OnErrors) == 0 {
			log.Info().Msg(semLogContext + " - adapting to OnErrors")
			cfg.OnErrors = []OnErrorPolicy{
				{ErrLevel: OnErrorLevelSystem, Policy: cfg.OnError},
				{ErrLevel: OnErrorLevelFatal, Policy: cfg.OnError},
				{ErrLevel: OnErrorLevelError, Policy: cfg.OnError},
			}
		} else {
			log.Info().Msg(semLogContext + " - OnErrors has been set, dropping OnError property")
		}
	}

	t := transformerProducerImpl{
		cfg:           cfg,
		quitc:         make(chan struct{}),
		monitorQuitc:  nil,
		txActive:      false,
		producers:     nil,
		consumer:      nil,
		partitionsCnt: 0,
		eofCnt:        0,
		processor:     processor,
		wg:            wg,
		metricLabels: map[string]string{
			"name": cfg.Name,
		},
	}

	if len(t.cfg.ToTopics) > 0 {
		t.monitorQuitc = make(chan struct{})
	}

	log.Info().Str(semLogTransformerProducerId, cfg.Name).Str("tick-interval", cfg.TickInterval.String()).Msg(semLogContext + " initializing tick interval")

	ctx, ctxCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ctxCancel()

	producerBrokers := cfg.CountDistinctProducerBrokers()

	isAutoCommit := false
	switch strings.ToLower(cfg.CommitMode) {
	case kafkalks.CommitModeAuto:
		t.cfg.ProducerId = ""
		isAutoCommit = true
	case kafkalks.CommitModeManual:
		t.cfg.ProducerId = ""
	case kafkalks.CommitModeTransaction:
		if t.cfg.ProducerId == "" || len(producerBrokers) > 1 {
			log.Warn().Str(semLogTransformerProducerId, cfg.Name).Str("producer-tx-id", t.cfg.ProducerId).Int("no-brokers", len(producerBrokers)).Msg(semLogContext + " commit-mode " + kafkalks.CommitModeTransaction + " not compatible with missing producer-tx-id or multiple brokers.. reverting to " + kafkalks.CommitModeAuto)
			isAutoCommit = true
			cfg.CommitMode = kafkalks.CommitModeAuto
		}
	default:
		log.Warn().Str(semLogTransformerProducerId, cfg.Name).Msg(semLogContext + " commit-mode not set....setting to " + kafkalks.CommitModeTransaction)
		isAutoCommit = true
		cfg.CommitMode = kafkalks.CommitModeAuto
	}

	log.Info().Str(semLogTransformerProducerId, cfg.Name).Str("tx-id", t.cfg.ProducerId).Bool("auto-commit", isAutoCommit).Msg(semLogContext + " transform producer: setting commit params")
	if len(producerBrokers) > 0 {
		t.producers = make(map[string]KafkaProducerWrapper)
	} else {
		log.Warn().Msg(semLogContext + " no output topics configured...")
	}

	if len(producerBrokers) > 1 {
		cfg.WorkMode = WorkModeMsg
	}

	for _, brokerName := range producerBrokers {

		kp, err := NewKafkaProducerWrapper2(ctx, brokerName, t.cfg.ProducerId, kafka.TopicPartition{Partition: kafka.PartitionAny}, cfg.WithSynchDelivery())
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
		t.producers[brokerName] = kp

		/*
			p, err := kafkalks.NewKafkaProducer(ctx, brokerName, t.cfg.ProducerId, kafka.TopicPartition{Partition: kafka.PartitionAny})
			if err != nil {
				return nil, err
			}

			var deliveryChannel chan kafka.Event
			if cfg.WithSynchDelivery() {
				deliveryChannel = make(chan kafka.Event)
			}
			kp := NewKafkaProducerWrapper(cfg.Name, p, deliveryChannel)
			t.producers[brokerName] = kp
		*/
		if cfg.WorkMode == WorkModeBatch {
			t.msgProducer = NewMessageProducer(cfg.Name, kp, true, cfg.ToTopics, cfg.RefMetrics.GId)
		}
	}

	t.consumer, err = kafkalks.NewKafkaConsumer(util.StringCoalesce(t.cfg.FromTopic.BrokerName, t.cfg.BrokerName), t.cfg.GroupId, isAutoCommit)
	if err != nil {
		for _, p := range t.producers {
			p.Close()
		}
		return nil, err
	}

	return &t, nil
}
