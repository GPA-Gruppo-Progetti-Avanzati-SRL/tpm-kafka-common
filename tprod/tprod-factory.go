package tprod

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
	"sync"
	"tpm-kafka-common/kafkalks"

	"strings"
	"time"
)

func NewTransformerProducer(cfg *TransformerProducerConfig, wg *sync.WaitGroup, processor TransformerProducerProcessor) (TransformerProducer, error) {

	mr, err := promutil.InitMetricsRegistry(cfg.Metrics)
	if err != nil {
		log.Error().Err(err).Msg("error creating metrics")
		return nil, err
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
		metrics:       mr,
		processor:     processor,
		wg:            wg,
	}

	if len(t.cfg.ToTopics) > 0 {
		t.monitorQuitc = make(chan struct{})
	}

	log.Info().Str(semLogTransformerProducerId, cfg.Name).Str("tick-interval", cfg.TickInterval.String()).Msg("Initializing tick interval")

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
			log.Warn().Str(semLogTransformerProducerId, cfg.Name).Str("producer-tx-id", t.cfg.ProducerId).Int("no-brokers", len(producerBrokers)).Msg("commit-mode " + kafkalks.CommitModeTransaction + " not compatible with missing producer-tx-id or multiple brokers.. reverting to " + kafkalks.CommitModeAuto)
			isAutoCommit = true
			cfg.CommitMode = kafkalks.CommitModeAuto
		}
	default:
		log.Warn().Str(semLogTransformerProducerId, cfg.Name).Msg("transform producer: commit-mode not set....setting to " + kafkalks.CommitModeTransaction)
		isAutoCommit = true
		cfg.CommitMode = kafkalks.CommitModeAuto
	}

	log.Info().Str(semLogTransformerProducerId, cfg.Name).Str("tx-id", t.cfg.ProducerId).Bool("auto-commit", isAutoCommit).Msg("transform producer: setting commit params")
	if len(producerBrokers) > 0 {
		t.producers = make(map[string]*kafka.Producer)
	} else {
		log.Warn().Msg("no output topics configured...")
	}

	for _, brokerName := range producerBrokers {
		p, err := kafkalks.NewKafkaProducer(ctx, brokerName, t.cfg.ProducerId)
		if err != nil {
			return nil, err
		}
		t.producers[brokerName] = p
	}

	t.consumer, err = kafkalks.NewKafkaConsumer(util.Coalesce(t.cfg.FromTopic.BrokerName, t.cfg.BrokerName), t.cfg.GroupId, isAutoCommit)
	if err != nil {
		for _, p := range t.producers {
			p.Close()
		}
		return nil, err
	}

	return &t, nil
}
