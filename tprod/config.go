package tprod

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
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
	TickInterval time.Duration              `yaml:"tick-interval,omitempty" mapstructure:"tick-interval,omitempty" json:"tick-interval,omitempty"`
	Exit         ConfigExitPolicy           `yaml:"exit" mapstructure:"exit" json:"exit"`
	Metrics      promutil.MetricGroupConfig `yaml:"metrics" mapstructure:"metrics" json:"metrics"`
	CommitMode   string                     `yaml:"commit-mode,omitempty" mapstructure:"commit-mode,omitempty" json:"commit-mode,omitempty"`
	GroupId      string                     `yaml:"consumer-group-id,omitempty" mapstructure:"consumer-group-id,omitempty" json:"consumer-group-id,omitempty"`
	ProducerId   string                     `yaml:"producer-tx-id,omitempty" mapstructure:"producer-tx-id,omitempty" json:"producer-tx-id,omitempty"`
	BrokerName   string                     `yaml:"broker-name,omitempty" mapstructure:"broker-name,omitempty" json:"broker-name,omitempty"`
	FromTopic    ConfigTopic                `yaml:"from-topic" mapstructure:"from-topic" json:"from-topic"`
	ToTopics     []ConfigTopic              `yaml:"to-topics,omitempty" mapstructure:"to-topics,omitempty" json:"to-topics,omitempty"`
	Tracing      TracingCfg                 `yaml:"tracing" mapstructure:"tracing" json:"tracing"`
}

type ConfigTopic struct {
	Id             string    `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	Name           string    `yaml:"name,omitempty" mapstructure:"name,omitempty" json:"name,omitempty"`
	BrokerName     string    `yaml:"broker-name,omitempty" mapstructure:"broker-name,omitempty" json:"broker-name,omitempty"`
	MaxPollTimeout int       `yaml:"max-poll-timeout,omitempty" mapstructure:"max-poll-timeout,omitempty" json:"max-poll-timeout,omitempty"`
	TopicType      TopicType `yaml:"type,omitempty" mapstructure:"type,omitempty" json:"type,omitempty"`
	MuteOn         bool      `yaml:"mute-on,omitempty" mapstructure:"mute-on,omitempty" json:"mute-on,omitempty"`
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
