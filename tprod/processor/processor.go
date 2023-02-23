package processor

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
)

type TransformerProducerProcessor interface {
	Process(km *kafka.Message, opts ...TransformerProducerProcessorOption) (Message, BAMData, error)
}

type TransformerProducerOptions struct {
	Span    opentracing.Span
	HarSpan hartracing.Span
}

type TransformerProducerProcessorOption func(opts *TransformerProducerOptions)

func TransformerProducerProcessorWithSpan(span opentracing.Span) TransformerProducerProcessorOption {
	return func(opts *TransformerProducerOptions) {
		opts.Span = span
	}
}

func TransformerProducerProcessorWithHarSpan(span hartracing.Span) TransformerProducerProcessorOption {
	return func(opts *TransformerProducerOptions) {
		opts.HarSpan = span
	}
}
