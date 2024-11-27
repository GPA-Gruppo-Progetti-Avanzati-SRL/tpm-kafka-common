package tprod

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
	"github.com/rs/zerolog/log"
	"sync"

	"strings"
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
		brokers:       nil,
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

	t.brokers = cfg.CountDistinctProducerBrokers()
	if len(t.brokers) > 0 {
		t.producers = make(map[string]KafkaProducerWrapper)
	} else {
		log.Warn().Msg(semLogContext + " no output topics configured...")
	}

	if len(t.brokers) > 1 {
		cfg.WorkMode = WorkModeMsg
	}

	isAutoCommit := false
	switch strings.ToLower(cfg.CommitMode) {
	case kafkalks.CommitModeAuto:
		t.cfg.ProducerId = ""
		isAutoCommit = true
	case kafkalks.CommitModeManual:
		t.cfg.ProducerId = ""
	case kafkalks.CommitModeTransaction:
		if t.cfg.ProducerId == "" || len(t.brokers) > 1 {
			log.Warn().Str(semLogTransformerProducerId, cfg.Name).Str("producer-tx-id", t.cfg.ProducerId).Int("no-brokers", len(t.brokers)).Msg(semLogContext + " commit-mode " + kafkalks.CommitModeTransaction + " not compatible with missing producer-tx-id or multiple brokers.. reverting to " + kafkalks.CommitModeAuto)
			isAutoCommit = true
			cfg.CommitMode = kafkalks.CommitModeAuto
		}
	default:
		log.Warn().Str(semLogTransformerProducerId, cfg.Name).Msg(semLogContext + " commit-mode not set....setting to " + kafkalks.CommitModeTransaction)
		isAutoCommit = true
		cfg.CommitMode = kafkalks.CommitModeAuto
	}

	log.Info().Str(semLogTransformerProducerId, cfg.Name).Str("tx-id", t.cfg.ProducerId).Bool("auto-commit", isAutoCommit).Msg(semLogContext + " transform producer: setting commit params")

	/*
		err = t.createProducers()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext + " creating producers failed")
			return nil, err
		}
	*/

	t.consumerBrokerName = util.StringCoalesce(t.cfg.FromTopic.BrokerName, t.cfg.BrokerName)
	t.consumer, err = kafkalks.NewKafkaConsumer(t.consumerBrokerName, t.cfg.GroupId, isAutoCommit)
	if err != nil {
		/*
			for _, p := range t.producers {
				p.Close()
			}
		*/
		logKafkaError(err).Msg(semLogContext + " consumer creation failed")
		return nil, err
	}

	return &t, nil
}
