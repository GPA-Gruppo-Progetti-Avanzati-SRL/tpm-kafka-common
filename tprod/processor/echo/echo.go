package echo

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
	"sync"
)

const (
	KMContentType = "content-type"
)

const (
	MetricsLabelIso20022DataSet = "ce_dataset"
	MetricsLabelStatusCode      = "status_code"
	MetricsLabelStatusRsn       = "status_rsn"

	MetricsIdDuration       = "duration"
	MetricsIdErrors         = "errors"
	MetricsIdMessages       = "messages"
	MetricsIdMessageDropped = "messages_dropped"
)

type Config struct {
	TransformerProducerConfig *tprod.TransformerProducerConfig `yaml:"t-prod,omitempty" mapstructure:"t-prod,omitempty" json:"t-prod,omitempty"`
	ProcessorConfig           *ProcessorConfig                 `yaml:"process,omitempty" mapstructure:"process,omitempty" json:"process,omitempty"`
}

type ProcessorConfig struct {
	NumRetries                 int    `yaml:"num-retries,omitempty" mapstructure:"num-retries,omitempty" json:"num-retries,omitempty"`
	NumberOfAttemptsHeaderName string `yaml:"no-attempts-header,omitempty" mapstructure:"no-attempts-header,omitempty" json:"no-attempts-header,omitempty"`
}

type echoImpl struct {
	tprod.UnimplementedTransformerProducerProcessor
	tprod.TransformerProducer
	cfg *Config

	batch []*kafka.Message
}

func NewEcho(cfg *Config, wg *sync.WaitGroup) (tprod.TransformerProducer, error) {
	var err error
	b := echoImpl{cfg: cfg}
	b.TransformerProducer, err = tprod.NewTransformerProducer(cfg.TransformerProducerConfig, wg, &b)
	return &b, err
}

func (b *echoImpl) ProcessMessage(km *kafka.Message, opts ...tprod.TransformerProducerProcessorOption) ([]tprod.Message, tprod.BAMData, error) {
	const semLogContext = "echo-t-prod::process"

	tprodOpts := tprod.TransformerProducerOptions{}
	for _, o := range opts {
		o(&tprodOpts)
	}

	bamData := tprod.BAMData{}
	bamData.AddLabel("test_label", "test_value")
	req, err := newRequestIn(km, tprodOpts.Span)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext + " deadletter message not resubmittable.... need a terminal dlt?")
		return nil, bamData, err
	}

	if b.cfg.ProcessorConfig.NumRetries >= 0 {
		if b.cfg.ProcessorConfig.NumberOfAttemptsHeaderName == "" {
			b.cfg.ProcessorConfig.NumberOfAttemptsHeaderName = CENumberOfAttempts
		}

		numberOfAttempts := req.GetHeaderAsInt(b.cfg.ProcessorConfig.NumberOfAttemptsHeaderName)
		if numberOfAttempts > b.cfg.ProcessorConfig.NumRetries {
			log.Error().Int("number-of-attempts", numberOfAttempts).Int("num-retries", b.cfg.ProcessorConfig.NumRetries).Msg(semLogContext + " reached max number of retries")
			return nil, bamData, nil
		}

		req.Headers[b.cfg.ProcessorConfig.NumberOfAttemptsHeaderName] = fmt.Sprint(numberOfAttempts + 1)
	}

	return []tprod.Message{{
		Span:    req.Span,
		ToTopic: tprod.TargetTopic{TopicType: "std"},
		Headers: req.Headers,
		Key:     req.Key,
		Body:    req.Body,
	}}, bamData, nil
}

func (b *echoImpl) AddMessage2Batch(km *kafka.Message, msgProducer tprod.MessageProducer) error {
	// Shoild do a startSpan 	s := tp.startSpan(km)
	const semLogContext = "echo-t-prod::add-to-batch"
	b.batch = append(b.batch, km)
	return nil
}

func (b *echoImpl) ProcessBatch(mp tprod.MessageProducer) error {
	const semLogContext = "echo-t-prod::process-batch"
	for _, km := range b.batch {
		err := mp.Produce(tprod.Message{
			HarSpan: nil,
			Span:    nil,
			ToTopic: tprod.TargetTopic{
				TopicType: "std",
			},
			Headers: nil,
			Key:     km.Key,
			Body:    km.Value,
		})

		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}
	}
	return nil
}

func (b *echoImpl) BatchSize() int {
	const semLogContext = "echo-t-prod::batch-size"
	return len(b.batch)
}

func (b *echoImpl) Clear() {
	const semLogContext = "echo-t-prod::clear"
	b.batch = nil
}
