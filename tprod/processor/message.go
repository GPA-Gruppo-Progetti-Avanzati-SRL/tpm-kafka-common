package processor

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
)

type TopicType string

const (
	TopicTypeStd        TopicType = "std"
	TopicTypeDeadLetter TopicType = "dead-letter"
)

type TargetTopic struct {
	Name      string    `yaml:"name" mapstructure:"name" json:"name"`
	TopicType TopicType `yaml:"type" mapstructure:"type" json:"type"`
}

type Message struct {
	HarSpan hartracing.Span
	Span    opentracing.Span
	ToTopic TargetTopic
	Headers map[string]string
	Key     []byte
	Body    []byte
}

func (m Message) IsZero() bool {
	return len(m.Body) == 0
}

func (m Message) ShowInfo() {
	log.Info().Str("type", string(m.ToTopic.TopicType)).Str("name", string(m.ToTopic.Name)).Int("value-size", len(m.Body)).Str("key", string(m.Key)).Msg("message info")
	for hn, hv := range m.Headers {
		log.Info().Str("header-name", hn).Str("header-value", hv).Msg("message header")
	}
}
