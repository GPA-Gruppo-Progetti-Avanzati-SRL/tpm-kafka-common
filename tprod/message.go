package tprod

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
)

type TopicType string

const (
	TopicTypeStd        TopicType = "std"
	TopicTypeDeadLetter TopicType = "dead-letter"
)

type TargetTopic struct {
	Id        string    `yaml:"id" mapstructure:"id" json:"id"`
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

func ToMessageHeaders(hs []kafka.Header) map[string]string {
	if len(hs) == 0 {
		return nil
	}

	headers := make(map[string]string)
	for _, h := range hs {
		headers[h.Key] = string(h.Value)
	}

	return headers
}

func (m Message) IsZero() bool {
	return len(m.Body) == 0
}

func (m Message) ShowInfo() {
	log.Info().Str("type", string(m.ToTopic.TopicType)).Str("name", string(m.ToTopic.Id)).Int("value-size", len(m.Body)).Str("key", string(m.Key)).Msg("message info")
	for hn, hv := range m.Headers {
		log.Info().Str("header-name", hn).Str("header-value", hv).Msg("message header")
	}
}
