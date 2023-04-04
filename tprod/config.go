package tprod

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod/processor"
	"github.com/rs/zerolog/log"
	"time"
)

type ConfigExitPolicy struct {
	OnFail    bool `yaml:"on-fail" mapstructure:"on-fail" json:"on-fail"`
	OnEof     bool `yaml:"on-eof" mapstructure:"on-eof" json:"on-eof"`
	EofAfterN int  `yaml:"eof-after-n,omitempty" mapstructure:"eof-after-n,omitempty" json:"eof-after-n,omitempty"`
}

type TracingCfg struct {
	SpanName string `yaml:"span-name" mapstructure:"span-name" json:"span-name"`
}

type TransformerProducerConfig struct {
	Name         string                     `yaml:"name" mapstructure:"name" json:"name"`
	TickInterval time.Duration              `yaml:"tick-interval" mapstructure:"tick-interval" json:"tick-interval"`
	Exit         ConfigExitPolicy           `yaml:"exit" mapstructure:"exit" json:"exit"`
	Metrics      promutil.MetricGroupConfig `yaml:"metrics" mapstructure:"metrics" json:"metrics"`
	CommitMode   string                     `yaml:"commit-mode" mapstructure:"commit-mode" json:"commit-mode"`
	GroupId      string                     `yaml:"consumer-group-id" mapstructure:"consumer-group-id" json:"consumer-group-id"`
	ProducerId   string                     `yaml:"producer-tx-id" mapstructure:"producer-tx-id" json:"producer-tx-id"`
	BrokerName   string                     `yaml:"broker-name,omitempty" mapstructure:"broker-name,omitempty" json:"broker-name,omitempty"`
	FromTopic    ConfigTopic                `yaml:"from-topic" mapstructure:"from-topic" json:"from-topic"`
	ToTopics     []ConfigTopic              `yaml:"to-topics" mapstructure:"to-topics" json:"to-topics"`
	Tracing      TracingCfg                 `yaml:"tracing" mapstructure:"tracing" json:"tracing"`
}

type ConfigTopic struct {
	Name           string              `yaml:"name" mapstructure:"name" json:"name"`
	BrokerName     string              `yaml:"broker-name,omitempty" mapstructure:"broker-name,omitempty" json:"broker-name,omitempty"`
	MaxPollTimeout int                 `yaml:"max-poll-timeout" mapstructure:"max-poll-timeout" json:"max-poll-timeout"`
	TopicType      processor.TopicType `yaml:"type" mapstructure:"type" json:"type"`
}

func (cfg *TransformerProducerConfig) CountDistinctProducerBrokers() []string {

	const semLogContext = "t-prod-config::count-distinct-brokers"
	m := make(map[string]struct{})
	for _, t := range cfg.ToTopics {
		n := util.Coalesce(t.BrokerName, cfg.BrokerName)
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

func (cfg *TransformerProducerConfig) FindTopic(n string) (int, error) {
	for i, p := range cfg.ToTopics {
		if p.Name == n {
			return i, nil
		}
	}

	return -1, fmt.Errorf("cannot find topic by name %s", n)
}

func (cfg *TransformerProducerConfig) FindTopicByType(topicType processor.TopicType) (int, error) {
	for i, p := range cfg.ToTopics {
		if p.TopicType == topicType {
			return i, nil
		}
	}

	return -1, fmt.Errorf("cannot find topic by type %s", topicType)
}

func (cfg *TransformerProducerConfig) CountTopicsByType(topicType processor.TopicType) int {
	numTopics := 0
	for _, p := range cfg.ToTopics {
		if p.TopicType == topicType {
			numTopics++
		}
	}

	return numTopics
}
