package tprod

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
)

type TransformerProducerProcessor interface {
	ProcessMessage(km *kafka.Message, opts ...TransformerProducerProcessorOption) ([]Message, BAMData, error)
	AddMessage2Batch(km *kafka.Message, msgProducer MessageProducer) error
	ProcessBatch(mp MessageProducer) error
	Clear()
	BatchSize() int
}

type UnimplementedTransformerProducerProcessor struct {
}

func (b *UnimplementedTransformerProducerProcessor) ProcessMessage(km *kafka.Message, opts ...TransformerProducerProcessorOption) ([]Message, BAMData, error) {
	const semLogContext = "t-prod-processor::process-message"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) AddMessage2Batch(km *kafka.Message, msgProducer MessageProducer) error {
	const semLogContext = "t-prod-processor::add-to-batch"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) ProcessBatch(mp MessageProducer) error {
	const semLogContext = "t-prod-processor::process-batch"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) BatchSize() int {
	const semLogContext = "t-prod-processor::batch-size"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) Clear() {
	const semLogContext = "t-prod-processor::clear"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
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
