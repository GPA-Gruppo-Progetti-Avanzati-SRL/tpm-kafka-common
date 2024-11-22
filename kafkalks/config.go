package kafkalks

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/rs/zerolog/log"
	"time"
)

const (
	AcksPropertyName                             = "acks"
	AutoOffsetResetPropertyName                  = "auto.offset.reset"
	BootstrapServersPropertyName                 = "bootstrap.servers"
	CommitModeAuto                               = "auto"
	CommitModeManual                             = "manual"
	CommitModeTransaction                        = "tx"
	ConnectionsMaxIdleMs                         = "connections.max.idle.ms"
	DeliveryTimeoutMs                            = "delivery.timeout.ms"
	EnableAutoCommitPropertyName                 = "enable.auto.commit"
	EnablePartitionEOFPropertyName               = "enable.partition.eof"
	EnableSSLCertificateVerificationPropertyName = "enable.ssl.certificate.verification"
	GoApplicationRebalanceEnablePropertyName     = "go.application.rebalance.enable"
	GroupIdPropertyName                          = "group.id"
	HeartBeatIntervalMs                          = "heartbeat.interval.ms"
	IsolationLevelPropertyName                   = "isolation.level"
	LingerMs                                     = "linger.ms"
	MaxPollIntervalMs                            = "max.poll.interval.ms"
	MessageSendMaxRetries                        = "message.send.max.retries"
	MetadataMaxAgeMs                             = "metadata.max.age.ms" // 180000
	MetadataMaxIdleMs                            = "metadata.max.idle.ms"
	RequestTimeoutMs                             = "request.timeout.ms" //60000
	Retries                                      = "retries"
	SASLMechanismPropertyName                    = "sasl.mechanism"
	SASLPasswordPropertyName                     = "sasl.password"
	SASLUsernamePropertyName                     = "sasl.username"
	SSLCaLocationPropertyName                    = "ssl.ca.location"
	SecurityProtocolPropertyName                 = "security.protocol"
	SessionTimeOutMsPropertyName                 = "session.timeout.ms"
	SocketKeepaliveEnable                        = "socket.keepalive.enable" // true
	TransactionalIdPropertyName                  = "transactional.id"
	TransactionalTimeoutMsPropertyName           = "transaction.timeout.ms"
	TransactionalMaxTimeoutMsPropertyName        = "transaction.max.timeout.ms"
	Debug                                        = "debug"

	KafkaNumberOfDeliveryAttemptsHeaderName = "Kafka-Delivery-Attempts"
)

type ConsumerConfig struct {
	// Consumer related configs
	IsolationLevel        string `mapstructure:"isolation-level" json:"isolation-level" yaml:"isolation-level"`
	MaxPollRecords        int    `mapstructure:"max-poll-records" json:"max-poll-records" yaml:"max-poll-records"`
	AutoOffsetReset       string `mapstructure:"auto-offset-reset" json:"auto-offset-reset" yaml:"auto-offset-reset"`
	SessionTimeoutMs      int    `mapstructure:"session-timeout-ms" json:"session-timeout-ms" yaml:"session-timeout-ms"`
	FetchMinBytes         int    `mapstructure:"fetch-min-bytes" json:"fetch-min-bytes" yaml:"fetch-min-bytes"`
	FetchMaxBytes         int    `mapstructure:"fetch-max-bytes" json:"fetch-max-bytes" yaml:"fetch-max-bytes"`
	Delay                 int    `mapstructure:"delay" json:"delay" yaml:"delay"`
	MaxRetry              int    `mapstructure:"max-retry" json:"max-retry" yaml:"max-retry"`
	EnablePartitionEOF    bool   `mapstructure:"enable-partition-eof" json:"enable-partition-eof" yaml:"enable-partition-eof"`
	MetadataMaxAgeMs      int    `mapstructure:"metadata-max-age-ms,omitempty" json:"metadata-max-age-ms,omitempty" yaml:"metadata-max-age-ms,omitempty"`
	SocketKeepaliveEnable bool   `mapstructure:"socket-keepalive-enable,omitempty" json:"socket-keepalive-enable,omitempty" yaml:"socket-keepalive-enable,omitempty"`
	// RequestTimeoutMs      int    `mapstructure:"request-timeout-ms,omitempty" json:"request-timeout-ms,omitempty" yaml:"request-timeout-ms,omitempty"`
	ConnectionsMaxIdleMs int `mapstructure:"connections-max-idle-ms,omitempty" json:"connections-max-idle-ms,omitempty" yaml:"connections-max-idle-ms,omitempty"`
	HeartBeatIntervalMs  int `mapstructure:"heartbeat-interval-ms,omitempty" json:"heartbeat-interval-ms,omitempty" yaml:"heartbeat-interval-ms,omitempty"`
}

type ProducerConfig struct {
	// Producer related configs
	Acks                    string                          `mapstructure:"acks" json:"acks" yaml:"acks"`
	TransactionTimeoutMs    int                             `mapstructure:"transaction-timeout-ms,omitempty" json:"transaction-timeout-ms,omitempty" yaml:"transaction-timeout-ms,omitempty"`
	TransactionMaxTimeoutMs int                             `mapstructure:"transaction-max-timeout-ms,omitempty" json:"transaction-max-timeout-ms,omitempty" yaml:"transaction.max-timeout-ms,omitempty"`
	DeliveryTimeout         time.Duration                   `mapstructure:"delivery-timeout"`
	FlushTimeout            time.Duration                   `mapstructure:"flush-timeout"`
	MessageSendMaxRetries   int                             `mapstructure:"max-retries"`
	AsyncDeliveryMetrics    promutil.MetricsConfigReference `mapstructure:"async-delivery-metrics,omitempty" yaml:"async-delivery-metrics,omitempty" json:"async-delivery-metrics,omitempty"`
	MetadataMaxAgeMs        int                             `mapstructure:"metadata-max-age-ms,omitempty" json:"metadata-max-age-ms,omitempty" yaml:"metadata-max-age-ms,omitempty"`
	SocketKeepaliveEnable   bool                            `mapstructure:"socket-keepalive-enable,omitempty" json:"socket-keepalive-enable,omitempty" yaml:"socket-keepalive-enable,omitempty"`
	RequestTimeoutMs        int                             `mapstructure:"request-timeout-ms,omitempty" json:"request-timeout-ms,omitempty" yaml:"request-timeout-ms,omitempty"`
	ConnectionsMaxIdleMs    int                             `mapstructure:"connections-max-idle-ms,omitempty" json:"connections-max-idle-ms,omitempty" yaml:"connections-max-idle-ms,omitempty"`
	MetadataMaxIdleMs       int                             `mapstructure:"metadata-max-idle-ms,omitempty" json:"metadata-max-idle-ms,omitempty" yaml:"metadata-max-idle-ms,omitempty"`
}

var DefaultProducerMetrics = promutil.MetricsConfigReference{
	GId:         "-",
	CounterId:   "-",
	HistogramId: "-",
	GaugeId:     "-",
}

type SSLCfg struct {
	CaLocation string `mapstructure:"ca-location" json:"ca-location" yaml:"ca-location"`
	SkipVerify bool   `json:"skv,omitempty" yaml:"skv,omitempty" mapstructure:"skv,omitempty"`
}

type SaslCfg struct {
	Mechanisms string `mapstructure:"mechanisms" json:"mechanisms" yaml:"mechanisms"`
	Username   string `mapstructure:"username" json:"username" yaml:"username"`
	Password   string `mapstructure:"password" json:"password" yaml:"password"`
	CaLocation string `json:"ca-location" mapstructure:"ca-location" yaml:"ca-location"`
	SkipVerify bool   `json:"skv,omitempty" mapstructure:"skv,omitempty" yaml:"skv,omitempty"`
}

type Config struct {
	BrokerName       string         `mapstructure:"broker-name" json:"broker-name" yaml:"broker-name"`
	BootstrapServers string         `mapstructure:"bootstrap-servers" json:"bootstrap-servers" yaml:"bootstrap-servers"`
	SecurityProtocol string         `mapstructure:"security-protocol" json:"security-protocol" yaml:"security-protocol"`
	SSL              SSLCfg         `mapstructure:"ssl" json:"ssl" yaml:"ssl"`
	SASL             SaslCfg        `mapstructure:"sasl" json:"sasl" yaml:"sasl"`
	Consumer         ConsumerConfig `mapstructure:"consumer" json:"consumer" yaml:"consumer"`
	Producer         ProducerConfig `mapstructure:"producer" json:"producer" yaml:"producer"`
	Debug            string         `mapstructure:"debug" json:"debug" yaml:"debug"`
	//TickInterval     string         `mapstructure:"tick-interval"`
	//Exit             struct {
	//	OnFail bool `mapstructure:"on-fail"`
	//	OnEof  bool `mapstructure:"on-eof"`
	//}
}

func (c *Config) PostProcess() error {

	c.BootstrapServers = util.ResolveConfigValueToString(c.BootstrapServers)
	c.SASL.Username = util.ResolveConfigValueToString(c.SASL.Username)
	c.SASL.Password = util.ResolveConfigValueToString(c.SASL.Password)
	c.SASL.Mechanisms = util.ResolveConfigValueToString(c.SASL.Mechanisms)

	return nil
}

func IsTransactionCommit(m string) bool {
	return m == CommitModeTransaction
}

func IsManuaCommit(m string) bool {
	return m == CommitModeManual
}

func IsAutoCommit(m string) bool {
	if m == CommitModeAuto {
		return true
	}

	if m == "" {
		log.Warn().Str("commit-mode", m).Msg("commit mode unset... defaulting to " + CommitModeAuto)
		return true
	}

	return false
}
