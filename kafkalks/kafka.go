package kafkalks

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"strings"
)

/*
 * Note the singleton mode has been disabled. I should probably need to think to something different. May be there is sort of config? At the linked service level...?
 * May be I need to name producers and if don't just create a new one...?
 */

type LinkedService struct {
	cfg      Config
	producer *kafka.Producer
}

func (lks *LinkedService) Name() string {
	return lks.cfg.BrokerName
}

func NewKafkaServiceInstanceWithConfig(cfg Config) (*LinkedService, error) {
	lks := LinkedService{cfg: cfg}
	return &lks, nil
}

func (lks *LinkedService) NewProducer(ctx context.Context, transactionalId string) (*kafka.Producer, error) {
	const semLogContext = "kafka-lks::new-producer"

	if lks.producer != nil {
		return lks.producer, nil
	}

	cfgMap2 := kafka.ConfigMap{
		BootstrapServersPropertyName: lks.cfg.BootstrapServers,
		AcksPropertyName:             lks.cfg.Producer.Acks,
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

	//lks.producer = producer
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

// Produce2Topic uses an internal producer out of any transaction and keep it in the linkedService for further processing.
func (lks *LinkedService) Produce2TopicToBeRevisedBecauseOfSingleton(topicName string, k, msg []byte, hdrs map[string]string, span opentracing.Span) error {

	log.Trace().Str("broker", lks.cfg.BrokerName).Str("topic", topicName).Msg("producing message")

	var err error
	if lks.producer == nil {
		lks.producer, err = lks.NewProducer(context.Background(), "")
		if err != nil {
			return err
		}
	}

	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Key:            k,
		Value:          msg,
	}

	var headers map[string]string
	if span != nil || len(hdrs) > 0 {
		headers = make(map[string]string)
	}

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

	if err := lks.producer.Produce(km, nil); err != nil {
		log.Error().Err(err).Msg("errors in producing message")
		return err
	}

	return nil
}
