package tprod

import (
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/rs/zerolog/log"
	"strings"
	"time"
)

const (
	OnErrorLevelFatal  = "fatal"
	OnErrorLevelSystem = "system"
	OnErrorLevelError  = "error"

	OnErrorExit       = "exit"
	OnErrorDeadLetter = "dead-letter"

	OnEofExit = "exit"

	WorkModeMsg   = "msg-mode"
	WorkModeBatch = "batch-mode"
)

type ServerConfig struct {
	// Exit              ConfigExitPolicy `yaml:"exit" mapstructure:"exit" json:"exit"`
	OnWorkerTerminated string `yaml:"on-worker-terminated,omitempty" mapstructure:"on-worker-terminated,omitempty" json:"on-worker-terminated,omitempty"` // Possible values: dead-letter, exit
	EnabledProcessors  string `yaml:"enabled-processors,omitempty" mapstructure:"enabled-processors,omitempty" json:"enabled-processors,omitempty"`
	StartDelay         int    `yaml:"start-delay-ms" mapstructure:"start-delay-ms" json:"start-delay-ms"`
}

func (sCfg *ServerConfig) IsProcessorEnabled(n string) bool {
	if sCfg.EnabledProcessors == "" || strings.Contains(sCfg.EnabledProcessors, n) {
		return true
	}

	return false
}

/*
type ConfigExitPolicy struct {
	OnFail    bool `yaml:"on-fail" mapstructure:"on-fail" json:"on-fail"`
	OnEof     bool `yaml:"on-eof" mapstructure:"on-eof" json:"on-eof"`
	EofAfterN int  `yaml:"eof-after-n,omitempty" mapstructure:"eof-after-n,omitempty" json:"eof-after-n,omitempty"`
}
*/

type TracingCfg struct {
	SpanName string `yaml:"span-name" mapstructure:"span-name" json:"span-name"`
}

type OnErrorPolicy struct {
	ErrLevel string `yaml:"level,omitempty" mapstructure:"level,omitempty" json:"level,omitempty"`
	Policy   string `yaml:"policy,omitempty" mapstructure:"policy,omitempty" json:"policy,omitempty"`
}

type TransformerProducerConfig struct {
	Name                    string                           `yaml:"name" mapstructure:"name" json:"name"`
	WorkMode                string                           `yaml:"work-mode" mapstructure:"work-mode" json:"work-mode"`
	KafkaQueueBufferSize    int                              `yaml:"mp-buffer-size,omitempty" mapstructure:"mp-buffer-size,omitempty" json:"mp-buffer-size,omitempty"`
	TickInterval            time.Duration                    `yaml:"tick-interval,omitempty" mapstructure:"tick-interval,omitempty" json:"tick-interval,omitempty"`
	OnError                 string                           `yaml:"on-error,omitempty" mapstructure:"on-error,omitempty" json:"on-error,omitempty"` // Possible values: dead-letter, exit
	OnErrors                []OnErrorPolicy                  `yaml:"on-errors,omitempty" mapstructure:"on-errors,omitempty" json:"on-errors,omitempty"`
	OnEof                   string                           `yaml:"on-eof,omitempty" mapstructure:"on-eof,omitempty" json:"on-eof,omitempty"` // Possible values: exit
	EofAfterN               int                              `yaml:"eof-after-n,omitempty" mapstructure:"eof-after-n,omitempty" json:"eof-after-n,omitempty"`
	RefMetrics              *promutil.MetricsConfigReference `yaml:"ref-metrics"  mapstructure:"ref-metrics"  json:"ref-metrics"`
	CommitMode              string                           `yaml:"commit-mode,omitempty" mapstructure:"commit-mode,omitempty" json:"commit-mode,omitempty"`
	GroupId                 string                           `yaml:"consumer-group-id,omitempty" mapstructure:"consumer-group-id,omitempty" json:"consumer-group-id,omitempty"`
	ProducerId              string                           `yaml:"producer-tx-id,omitempty" mapstructure:"producer-tx-id,omitempty" json:"producer-tx-id,omitempty"`
	BrokerName              string                           `yaml:"broker-name,omitempty" mapstructure:"broker-name,omitempty" json:"broker-name,omitempty"`
	FromTopic               ConfigTopic                      `yaml:"from-topic" mapstructure:"from-topic" json:"from-topic"`
	ToTopics                []ConfigTopic                    `yaml:"to-topics,omitempty" mapstructure:"to-topics,omitempty" json:"to-topics,omitempty"`
	Tracing                 TracingCfg                       `yaml:"tracing" mapstructure:"tracing" json:"tracing"`
	StartDelay              int                              `yaml:"start-delay" mapstructure:"start-delay" json:"start-delay"`
	WithSynchDlv            string                           `yaml:"with-synch-delivery,omitempty" mapstructure:"with-synch-delivery,omitempty" json:"with-synch-delivery,omitempty"`
	NoAbortOnAsyncDlvFailed string                           `yaml:"no-abort-on-async-delivery-failed,omitempty" mapstructure:"no-abort-on-async-delivery-failed,omitempty" json:"no-abort-on-async-delivery-failed,omitempty"`
}

func (d *TransformerProducerConfig) WithSynchDelivery() bool {
	if d.WithSynchDlv == "" || d.WithSynchDlv == "true" {
		return true
	}

	return false
}

func (d *TransformerProducerConfig) NoAbortOnAsyncDeliveryFailed() bool {
	if d.NoAbortOnAsyncDlvFailed == "" || d.NoAbortOnAsyncDlvFailed == "true" {
		return true
	}

	return false
}

type ConfigTopic struct {
	Id             string    `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	Name           string    `yaml:"name,omitempty" mapstructure:"name,omitempty" json:"name,omitempty"`
	BrokerName     string    `yaml:"broker-name,omitempty" mapstructure:"broker-name,omitempty" json:"broker-name,omitempty"`
	MaxPollTimeout int       `yaml:"max-poll-timeout,omitempty" mapstructure:"max-poll-timeout,omitempty" json:"max-poll-timeout,omitempty"`
	TopicType      TopicType `yaml:"type,omitempty" mapstructure:"type,omitempty" json:"type,omitempty"`
	MuteOn         bool      `yaml:"mute-on,omitempty" mapstructure:"mute-on,omitempty" json:"mute-on,omitempty"`
}

func ErrorPolicyForError(err error, onErrors []OnErrorPolicy) string {

	level := OnErrorLevelFatal
	var tprodErr *TransformerProducerError
	if errors.As(err, &tprodErr) {
		level = tprodErr.Level
	}

	foundExit := false
	for _, c := range onErrors {
		if c.ErrLevel == level {
			return c.Policy
		}

		if c.Policy == OnErrorExit {
			foundExit = true
		}
	}

	if foundExit {
		return OnErrorExit
	}

	return OnErrorDeadLetter
}

func (cfg *TransformerProducerConfig) CountDistinctProducerBrokers() []string {

	const semLogContext = "t-prod-config::count-distinct-brokers"
	m := make(map[string]struct{})
	for _, t := range cfg.ToTopics {
		n := util.StringCoalesce(t.BrokerName, cfg.BrokerName)
		if _, ok := m[n]; !ok {
			m[n] = struct{}{}
		}
	}

	log.Info().Int("no-producers", len(m)).Msg(semLogContext)

	var brokerList []string
	for n, _ := range m {
		brokerList = append(brokerList, n)
	}
	return brokerList
}

func (cfg *TransformerProducerConfig) FindTopicByName(n string) (int, error) {
	for i, p := range cfg.ToTopics {
		if p.Name == n {
			return i, nil
		}
	}

	return -1, fmt.Errorf("cannot find topic by name %s", n)
}

func (cfg *TransformerProducerConfig) FindTopicById(id string) (int, error) {
	for i, p := range cfg.ToTopics {

		// Use name if id has not been set
		tid := p.Id
		if tid == "" {
			tid = p.Name
		}
		if tid == id {
			return i, nil
		}
	}

	return -1, fmt.Errorf("cannot find topic by id %s", id)
}

func (cfg *TransformerProducerConfig) FindTopicByIdOrType(topicId string, topicType TopicType) (int, error) {

	if topicId != "" {
		return cfg.FindTopicById(topicId)
	}

	return cfg.FindTopicByType(topicType)
}

func (cfg *TransformerProducerConfig) FindTopicByType(topicType TopicType) (int, error) {
	for i, p := range cfg.ToTopics {
		if p.TopicType == topicType {
			return i, nil
		}
	}

	return -1, fmt.Errorf("cannot find topic by type %s", topicType)
}

func (cfg *TransformerProducerConfig) CountTopicsByType(topicType TopicType) int {
	numTopics := 0
	for _, p := range cfg.ToTopics {
		if p.TopicType == topicType {
			numTopics++
		}
	}

	return numTopics
}
