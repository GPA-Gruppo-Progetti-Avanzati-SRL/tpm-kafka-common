package kafkalks

import (
	"context"
	"strings"
	"sync"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

/*
 * Note the singleton mode has been disabled. I should probably need to think to something different. May be there is sort of config? At the linked service level...?
 * May be I need to name producers and if don't just create a new one...?
 */

type SharedProducer struct {
	brokerName string
	producer   *kafka.Producer
}

type LinkedService struct {
	cfg            Config
	mu             sync.Mutex
	sharedProducer SharedProducer
}

func (lks *LinkedService) Config() Config {
	return lks.cfg
}

func (lks *LinkedService) Name() string {
	return lks.cfg.BrokerName
}

func (lks *LinkedService) Close() {

	const semLogContext = "kafka-lks::close"
	if lks.sharedProducer.producer != nil {
		timeoutMs := 1000
		if lks.cfg.Producer.FlushTimeout != 0 {
			timeoutMs = int(lks.cfg.Producer.FlushTimeout.Milliseconds())
		}

		log.Info().Int("timeout-ms", timeoutMs).Str("broker-name", lks.cfg.BrokerName).Msg(semLogContext + " closing shared producer")
		unf := lks.sharedProducer.producer.Flush(timeoutMs)
		for unf > 0 {
			log.Info().Int("timeout-ms", timeoutMs).Int("out-standing", unf).Msg(semLogContext + " outstanding events")
			unf = lks.sharedProducer.producer.Flush(timeoutMs)
		}
		lks.sharedProducer.producer.Close()
		lks.sharedProducer.producer = nil
	}

}

func NewKafkaServiceInstanceWithConfig(cfg Config) (*LinkedService, error) {

	// Initialize the metrics of the producer. These metrics are used at the moment by the shared producer in the monitor produced events.
	cfg.Producer.AsyncDeliveryMetrics = promutil.CoalesceMetricsConfig(cfg.Producer.AsyncDeliveryMetrics, DefaultProducerMetrics)
	lks := LinkedService{cfg: cfg, sharedProducer: SharedProducer{brokerName: cfg.BrokerName}}
	return &lks, nil
}

func (lks *LinkedService) NewProducer(ctx context.Context, transactionalId string) (*kafka.Producer, error) {
	const semLogContext = "kafka-lks::new-producer"

	cfgMap2 := kafka.ConfigMap{
		BootstrapServersPropertyName: lks.cfg.BootstrapServers,
		AcksPropertyName:             lks.cfg.Producer.Acks,
	}

	if lks.cfg.Debug != "" {
		_ = cfgMap2.SetKey(Debug, lks.cfg.Debug)
	}

	/*
		switch {
		case lks.cfg.Producer.LingerMs == 0:
		case lks.cfg.Producer.LingerMs < 0:
			_ = cfgMap2.SetKey(LingerMs, 0)
		case lks.cfg.Producer.LingerMs > 0:
			_ = cfgMap2.SetKey(LingerMs, lks.cfg.Producer.LingerMs)
		}
	*/

	if lks.cfg.Producer.ConnectionsMaxIdleMs > 0 {
		_ = cfgMap2.SetKey(ConnectionsMaxIdleMs, lks.cfg.Producer.ConnectionsMaxIdleMs)
	}

	if lks.cfg.Producer.DeliveryTimeout != 0 {
		_ = cfgMap2.SetKey(DeliveryTimeoutMs, int(lks.cfg.Producer.DeliveryTimeout.Milliseconds()))
	}

	// A value of -1 triggers a set to zero. Not using the zero since its the default value for ints... and should match the default value of linger.
	// should be possible to set as pointer
	if lks.cfg.Producer.LingerMs != nil {
		_ = cfgMap2.SetKey(LingerMs, *lks.cfg.Producer.LingerMs)
	}

	if lks.cfg.Producer.MessageSendMaxRetries > 0 {
		_ = cfgMap2.SetKey(MessageSendMaxRetries, lks.cfg.Producer.MessageSendMaxRetries)
	}

	if lks.cfg.Producer.MetadataMaxAgeMs > 0 {
		_ = cfgMap2.SetKey(MetadataMaxAgeMs, lks.cfg.Producer.MetadataMaxAgeMs)
	}

	if lks.cfg.Producer.MetadataMaxIdleMs > 0 {
		_ = cfgMap2.SetKey(MetadataMaxIdleMs, lks.cfg.Producer.MetadataMaxIdleMs)
	}

	if lks.cfg.Producer.RequestTimeoutMs > 0 {
		_ = cfgMap2.SetKey(RequestTimeoutMs, lks.cfg.Producer.RequestTimeoutMs)
	}

	if lks.cfg.Producer.RetryBackOff != 0 {
		_ = cfgMap2.SetKey(RetryBackOffMs, int(lks.cfg.Producer.RetryBackOff.Milliseconds()))
	}

	if lks.cfg.Producer.SocketKeepaliveEnable {
		_ = cfgMap2.SetKey(SocketKeepaliveEnable, lks.cfg.Producer.SocketKeepaliveEnable)
	}

	if transactionalId != "" {
		_ = cfgMap2.SetKey(TransactionalIdPropertyName, transactionalId)
		_ = cfgMap2.SetKey(TransactionalTimeoutMsPropertyName, util.IntCoalesce(lks.cfg.Producer.TransactionTimeoutMs, -1))
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
			log.Info().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " ca-location not configured")
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
			log.Info().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " ca-location not configured")
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

	/* Seems to be producer only
	if lks.cfg.Consumer.RequestTimeoutMs > 0 {
		_ = cfgMap.SetKey(RequestTimeoutMs, lks.cfg.Consumer.RequestTimeoutMs)
	}
	*/

	if lks.cfg.Consumer.ConnectionsMaxIdleMs > 0 {
		_ = cfgMap.SetKey(ConnectionsMaxIdleMs, lks.cfg.Consumer.ConnectionsMaxIdleMs)
	}

	if lks.cfg.Consumer.HeartBeatIntervalMs > 0 {
		_ = cfgMap.SetKey(HeartBeatIntervalMs, lks.cfg.Consumer.HeartBeatIntervalMs)
	}

	if lks.cfg.Consumer.EnablePartitionEOF {
		log.Info().Msg(semLogContext + " enabling eof partitions notifications")
		_ = cfgMap.SetKey(EnablePartitionEOFPropertyName, lks.cfg.Consumer.EnablePartitionEOF)
	}

	if lks.cfg.Consumer.MetadataMaxAgeMs > 0 {
		_ = cfgMap.SetKey(MetadataMaxAgeMs, lks.cfg.Consumer.MetadataMaxAgeMs)
	}

	if lks.cfg.Consumer.PartitionAssignmentStrategy != "" {
		_ = cfgMap.SetKey(PartitionAssignmentStrategy, lks.cfg.Consumer.PartitionAssignmentStrategy)
	}

	if lks.cfg.Consumer.RetryBackOff != 0 {
		_ = cfgMap.SetKey(RetryBackOffMs, int(lks.cfg.Consumer.RetryBackOff.Milliseconds()))
	}

	if lks.cfg.Consumer.SocketKeepaliveEnable {
		_ = cfgMap.SetKey(SocketKeepaliveEnable, lks.cfg.Consumer.SocketKeepaliveEnable)
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
			log.Info().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " ca-location not configured")
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
			log.Info().Str(SecurityProtocolPropertyName, lks.cfg.SecurityProtocol).Msg(semLogContext + " ca-location not configured")
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
