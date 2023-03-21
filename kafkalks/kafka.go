package kafkalks

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"net/http"
	"strings"
	"sync"
)

/*
 * Note the singleton mode has been disabled. I should probably need to think to something different. May be there is sort of config? At the linked service level...?
 * May be I need to name producers and if don't just create a new one...?
 */

type SharedProducer struct {
	brokerName string
	producer   *kafka.Producer
	mu         sync.Mutex
}

type LinkedService struct {
	cfg            Config
	sharedProducer SharedProducer
}

func (lks *LinkedService) Name() string {
	return lks.cfg.BrokerName
}

func (lks *LinkedService) Close() {

	const semLogContext = "kafka-lks::close"
	if lks.sharedProducer.producer != nil {
		timeoutMs := 1000
		if lks.cfg.Producer.FlushTimeout == 0 {
			timeoutMs = int(lks.cfg.Producer.FlushTimeout.Milliseconds())
		}

		log.Info().Int("timeout-ms", timeoutMs).Str("broker-name", lks.cfg.BrokerName).Msg(semLogContext + " closing shared producer")
		unf := lks.sharedProducer.producer.Flush(timeoutMs)
		for unf > 0 {
			log.Info().Int("timeout-ms", timeoutMs).Int("out-standing", unf).Msg(semLogContext + " outstanding events")
		}
		lks.sharedProducer.producer.Close()
		lks.sharedProducer.producer = nil
	}

}

func NewKafkaServiceInstanceWithConfig(cfg Config) (*LinkedService, error) {
	lks := LinkedService{cfg: cfg, sharedProducer: SharedProducer{brokerName: cfg.BrokerName}}
	return &lks, nil
}

func (lks *LinkedService) NewProducer(ctx context.Context, transactionalId string) (*kafka.Producer, error) {
	const semLogContext = "kafka-lks::new-producer"

	cfgMap2 := kafka.ConfigMap{
		BootstrapServersPropertyName: lks.cfg.BootstrapServers,
		AcksPropertyName:             lks.cfg.Producer.Acks,
	}

	if lks.cfg.Producer.DeliveryTimeout != 0 {
		_ = cfgMap2.SetKey(DeliveryTimeoutMs, int(lks.cfg.Producer.DeliveryTimeout.Milliseconds()))
	}

	if transactionalId != "" {
		_ = cfgMap2.SetKey(TransactionalIdPropertyName, transactionalId)
		_ = cfgMap2.SetKey(TransactionalTimeoutMsPropertyName, lks.cfg.Producer.MaxTimeoutMs)
	} else {
		_ = cfgMap2.SetKey("enable.idempotence", true)
	}

	switch lks.cfg.SecurityProtocol {
	case "SSL":
		if lks.cfg.SSL.CaLocation != "" {
			_ = cfgMap2.SetKey(SecurityProtocolPropertyName, "SSL")
			_ = cfgMap2.SetKey(SSLCaLocationPropertyName, lks.cfg.SSL.CaLocation)
			_ = cfgMap2.SetKey(EnableSSLCertificateVerificationPropertyName, !lks.cfg.SSL.SkipVerify)
		} else {
			_ = cfgMap2.SetKey(EnableSSLCertificateVerificationPropertyName, false)
			log.Error().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " ca-location not configured")
		}
	case "SASL_SSL":
		fallthrough
	case "SASL":
		_ = cfgMap2.SetKey(SecurityProtocolPropertyName, "SASL_SSL")
		_ = cfgMap2.SetKey(SASLMechanismPropertyName, lks.cfg.SASL.Mechanisms)
		_ = cfgMap2.SetKey(SASLUsernamePropertyName, lks.cfg.SASL.Username)
		_ = cfgMap2.SetKey(SASLPasswordPropertyName, lks.cfg.SASL.Password)
		if lks.cfg.SASL.CaLocation != "" {
			_ = cfgMap2.SetKey(SSLCaLocationPropertyName, lks.cfg.SASL.CaLocation)
			_ = cfgMap2.SetKey(EnableSSLCertificateVerificationPropertyName, !lks.cfg.SASL.SkipVerify)
		} else {
			log.Error().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " ca-location not configured")
			_ = cfgMap2.SetKey(EnableSSLCertificateVerificationPropertyName, false)
		}
	default:
		log.Error().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " skipping security-protocol settings")
	}

	logConfigMap(semLogContext, cfgMap2)
	producer, err := kafka.NewProducer(&cfgMap2)

	if err != nil {
		log.Error().Err(err).Send()
		return nil, err
	}

	if transactionalId != "" {
		err = producer.InitTransactions(ctx)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext + " producer initialization")
			return nil, err
		}
	}

	return producer, nil
}

func logConfigMap(semLogContext string, m kafka.ConfigMap) {
	for n, v := range m {
		if strings.Contains(n, "username") || strings.Contains(n, "password") {
			v = "***********"
		}
		log.Info().Str("property", n).Interface("value", v).Msg(semLogContext + " kafka config map")
	}
}

func (lks *LinkedService) NewConsumer(groupId string, autoCommit bool) (*kafka.Consumer, error) {

	const semLogContext = "kafka-lks::new-consumer"
	log.Info().Msg(semLogContext)

	cfgMap := kafka.ConfigMap{
		BootstrapServersPropertyName:             lks.cfg.BootstrapServers,
		GroupIdPropertyName:                      groupId,
		AutoOffsetResetPropertyName:              lks.cfg.Consumer.AutoOffsetReset,
		SessionTimeOutMsPropertyName:             lks.cfg.Consumer.SessionTimeoutMs,
		EnableAutoCommitPropertyName:             autoCommit,
		IsolationLevelPropertyName:               lks.cfg.Consumer.IsolationLevel,
		GoApplicationRebalanceEnablePropertyName: true,
	}

	if lks.cfg.Consumer.EnablePartitionEOF {
		log.Info().Msg(semLogContext + " enabling eof partitions notifications")
		_ = cfgMap.SetKey(EnablePartitionEOFPropertyName, lks.cfg.Consumer.EnablePartitionEOF)
	}

	/*
		if lks.cfg.Exit.OnEof {
			log.Info().Msg("enabling eof partitions notifications")
			_ = cfgMap.SetKey(EnablePartitionEOFPropertyName, lks.cfg.Exit.OnEof)
		}
	*/

	switch lks.cfg.SecurityProtocol {
	case "SSL":
		if lks.cfg.SSL.CaLocation != "" {
			_ = cfgMap.SetKey(SecurityProtocolPropertyName, "SSL")
			_ = cfgMap.SetKey(SSLCaLocationPropertyName, lks.cfg.SSL.CaLocation)
			_ = cfgMap.SetKey(EnableSSLCertificateVerificationPropertyName, !lks.cfg.SSL.SkipVerify)
		} else {
			_ = cfgMap.SetKey(EnableSSLCertificateVerificationPropertyName, false)
			log.Error().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " ca-location not configured")
		}
	case "SASL_SSL":
		fallthrough
	case "SASL":
		_ = cfgMap.SetKey(SecurityProtocolPropertyName, "SASL_SSL")
		_ = cfgMap.SetKey(SASLMechanismPropertyName, lks.cfg.SASL.Mechanisms)
		_ = cfgMap.SetKey(SASLUsernamePropertyName, lks.cfg.SASL.Username)
		_ = cfgMap.SetKey(SASLPasswordPropertyName, lks.cfg.SASL.Password)
		if lks.cfg.SASL.CaLocation != "" {
			_ = cfgMap.SetKey(SSLCaLocationPropertyName, lks.cfg.SASL.CaLocation)
			_ = cfgMap.SetKey(EnableSSLCertificateVerificationPropertyName, !lks.cfg.SASL.SkipVerify)
		} else {
			log.Error().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " ca-location not configured")
			_ = cfgMap.SetKey(EnableSSLCertificateVerificationPropertyName, false)
		}
	default:
		log.Error().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " skipping security-protocol settings")
	}

	/*
		if lks.cfg.SSL.CaLocation != "" {
			_ = cfgMap.SetKey("security.protocol", "SSL")
			_ = cfgMap.SetKey("ssl.ca.location", lks.cfg.SSL.CaLocation)
		}

		if lks.cfg.SecurityProtocol == "SASL_SSL" {
			_ = cfgMap.SetKey("security.protocol", "SASL_SSL")
			_ = cfgMap.SetKey("sasl.mechanisms", lks.cfg.SASL.Mechanisms)
			_ = cfgMap.SetKey("sasl.username", lks.cfg.SASL.Username)
			_ = cfgMap.SetKey("sasl.password", lks.cfg.SASL.Password)
		}
	*/

	logConfigMap(semLogContext, cfgMap)
	consumer, err := kafka.NewConsumer(&cfgMap)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext + " consumer initialization error")
		return nil, err
	}

	return consumer, nil
}

/*
 * SharedProducer
 */

func (lks *LinkedService) NewSharedProducer(ctx context.Context, synchDelivery bool) (SharedProducer, error) {
	const semLogContext = "kafka-lks::new-sha-producer"

	var err error
	lks.sharedProducer.mu.Lock()
	if lks.sharedProducer.producer == nil {
		lks.sharedProducer.producer, err = lks.NewProducer(ctx, "")
		go lks.monitorSharedProducerAsyncEvents(lks.sharedProducer.producer)
	}
	lks.sharedProducer.mu.Unlock()
	return lks.sharedProducer, err
}

func (lks *LinkedService) monitorSharedProducerAsyncEvents(producer *kafka.Producer) {
	const semLogContext = "kafka-lks::monitor-sha-producer"
	log.Info().Msg(semLogContext + " starting monitor producer events")

	exitFromLoop := false
	for e := range producer.Events() {

		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Info().Interface("event", ev).Msg(semLogContext + " delivery failed")
			} else {
				log.Trace().Interface("partition", ev.TopicPartition).Msg(semLogContext + " delivered message")
			}
		}

		if exitFromLoop {
			break
		}
	}

	log.Info().Msg(semLogContext + " exiting from monitor producer events")
}

type ProducerResponse struct {
	BrokerName string       `mapstructure:"broker-name,omitempty" json:"broker-name,omitempty" yaml:"broker-name,omitempty"`
	Topic      string       `mapstructure:"topic,omitempty" json:"topic,omitempty" yaml:"topic,omitempty"`
	Status     int          `mapstructure:"status,omitempty" json:"status,omitempty" yaml:"status,omitempty"`
	Error      string       `mapstructure:"error,omitempty" json:"error,omitempty" yaml:"error,omitempty"`
	Offset     kafka.Offset `mapstructure:"offset,omitempty" json:"offset,omitempty" yaml:"offset,omitempty"`
	Partition  int32        `mapstructure:"partition,omitempty" json:"partition,omitempty" yaml:"partition,omitempty"`
}

func (shaProd *SharedProducer) Produce2Topic(topicName string, k, msg []byte, hdrs map[string]string, span opentracing.Span) ProducerResponse {
	const semLogContext = "kafka-lks::produce-2-topic"
	log.Trace().Str("broker", shaProd.brokerName).Str("topic", topicName).Msg(semLogContext)

	var err error
	if shaProd.producer == nil {
		err = errors.New("producer is nil")
		return ProducerResponse{Status: http.StatusInternalServerError, BrokerName: shaProd.brokerName, Error: err.Error(), Topic: topicName}
	}

	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Key:            k,
		Value:          msg,
	}

	var headers map[string]string
	if span != nil || len(hdrs) > 0 {
		headers = make(map[string]string)

		if span != nil {
			opentracing.GlobalTracer().Inject(
				span.Context(),
				opentracing.TextMap,
				opentracing.TextMapCarrier(headers))
		}

		for headerKey, headerValue := range hdrs {
			headers[headerKey] = headerValue
		}

		for headerKey, headerValue := range headers {
			km.Headers = append(km.Headers, kafka.Header{
				Key:   headerKey,
				Value: []byte(headerValue),
			})
		}
	}

	if err := shaProd.producer.Produce(km, nil); err != nil {
		log.Error().Err(err).Msg(semLogContext + " errors in producing message")
		return ProducerResponse{Status: http.StatusServiceUnavailable, BrokerName: shaProd.brokerName, Error: err.Error(), Topic: topicName}
	}

	return ProducerResponse{Status: http.StatusAccepted, BrokerName: shaProd.brokerName, Error: err.Error(), Topic: topicName}
}
