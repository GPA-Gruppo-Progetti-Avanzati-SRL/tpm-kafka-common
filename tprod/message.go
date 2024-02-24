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

	TopicIdDeadLetter = "dead-letter"
)

type TargetTopic struct {
	Id        string    `yaml:"id" mapstructure:"id" json:"id"`
	TopicType TopicType `yaml:"type" mapstructure:"type" json:"type"`
}

type Message struct {
	isBatchMessage  bool
	HarSpan         hartracing.Span
	Span            opentracing.Span
	ToTopic         TargetTopic
	Headers         map[string]string
	Key             []byte
	Body            []byte
	MessageProducer MessageProducer
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

func NewBatchMessage(n string, km *kafka.Message, opts ...MessageOption) Message {
	m := NewMessage(n, km, opts...)
	m.isBatchMessage = true
	return m
}

func NewMessage(n string, km *kafka.Message, opts ...MessageOption) Message {
	span, harSpan := requestSpans(n, km.Headers)

	tprodOpts := MessageOptions{}
	for _, o := range opts {
		o(&tprodOpts)
	}

	if tprodOpts.Span != nil {
		span = tprodOpts.Span
	}

	if tprodOpts.HarSpan != nil {
		harSpan = tprodOpts.HarSpan
	}

	return Message{
		HarSpan:         harSpan,
		Span:            span,
		Headers:         ToMessageHeaders(km.Headers),
		Key:             km.Key,
		Body:            km.Value,
		MessageProducer: tprodOpts.MessageProducer,
	}
}

func (m Message) Finish() {
	const semLogContextMsg = "message::finish"
	const semLogContextBatchMsg = "batch-message::finish"

	if m.Span != nil {
		m.Span.Finish()
		m.Span = nil
	}

	if m.HarSpan != nil {
		err := m.HarSpan.Finish()
		if err != nil {
			semLogContext := semLogContextMsg
			if m.isBatchMessage {
				semLogContext = semLogContextBatchMsg
			}
			log.Error().Err(err).Msg(semLogContext)
		}
		m.HarSpan = nil
	}
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

func requestSpans(spanName string, hs []kafka.Header) (opentracing.Span, hartracing.Span) {

	const semLogContext = "t-prod::request-span"

	/*
		spanName := tp.cfg.Tracing.SpanName

		if spanName == "" {
			spanName = tp.cfg.Name
		}
	*/

	if spanName == "" {
		spanName = "not-assigned"
		log.Warn().Msgf(semLogContext+" span name cannot be assigned...defaulting to '%s'", spanName)
	}

	headers := make(map[string]string)
	for _, header := range hs {
		headers[header.Key] = string(header.Value)
	}

	spanContext, _ := opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapCarrier(headers))

	var span opentracing.Span
	if spanContext != nil {
		span = opentracing.StartSpan(spanName, opentracing.FollowsFrom(spanContext))
	} else {
		span = opentracing.StartSpan(spanName)
	}

	harSpanContext, harSpanErr := hartracing.GlobalTracer().Extract("", hartracing.TextMapCarrier(headers))
	// log.Trace().Bool("span-from-message", spanContext != nil).Bool("har-span-from-message", harSpanErr == nil).Msg(semLogContext)

	var harSpan hartracing.Span
	if harSpanErr == nil {
		harSpan = hartracing.GlobalTracer().StartSpan(hartracing.ChildOf(harSpanContext))
	}

	return span, harSpan
}
