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
	tprod.TransformerProducer
	cfg *Config
}

func NewEcho(cfg *Config, wg *sync.WaitGroup) (tprod.TransformerProducer, error) {
	var err error
	b := echoImpl{cfg: cfg}
	b.TransformerProducer, err = tprod.NewTransformerProducer(cfg.TransformerProducerConfig, wg, &b)
	return &b, err
}

func (b *echoImpl) Process(km *kafka.Message, opts ...tprod.TransformerProducerProcessorOption) (tprod.Message, tprod.BAMData, error) {
	const semLogContext = "echo-tprod::process"

	tprodOpts := tprod.TransformerProducerOptions{}
	for _, o := range opts {
		o(&tprodOpts)
	}

	bamData := tprod.BAMData{}
	bamData.AddLabel("test_label", "test_value")

	req, err := newRequestIn(km, tprodOpts.Span)

	if err != nil {
		log.Error().Err(err).Msg(semLogContext + " deadletter message not resubmittable.... need a terminal dlt?")
		return tprod.Message{}, bamData, err
	}

	if b.cfg.ProcessorConfig.NumRetries >= 0 {
		if b.cfg.ProcessorConfig.NumberOfAttemptsHeaderName == "" {
			b.cfg.ProcessorConfig.NumberOfAttemptsHeaderName = CENumberOfAttempts
		}

		numberOfAttempts := req.GetNumberOfAttempts(b.cfg.ProcessorConfig.NumberOfAttemptsHeaderName)
		if numberOfAttempts > b.cfg.ProcessorConfig.NumRetries {
			log.Error().Int("number-of-attempts", numberOfAttempts).Int("num-retries", b.cfg.ProcessorConfig.NumRetries).Msg(semLogContext + " reached max number of retries")
			return tprod.Message{}, bamData, nil
		}

		req.Headers[b.cfg.ProcessorConfig.NumberOfAttemptsHeaderName] = fmt.Sprint(numberOfAttempts + 1)
	}

	return tprod.Message{
		Span:    req.Span,
		ToTopic: tprod.TargetTopic{TopicType: "std"},
		Headers: req.Headers,
		Key:     req.Key,
		Body:    req.Body,
	}, bamData, nil
}
