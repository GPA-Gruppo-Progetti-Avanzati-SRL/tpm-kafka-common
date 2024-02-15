package tprod

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
)

type TransformerProducerProcessor interface {
	ProcessMessage(m Message) ([]Message, BAMData, error)
	AddMessage2Batch(m Message) error
	ProcessBatch() error
	Clear()
	BatchSize() int
}

type UnimplementedTransformerProducerProcessor struct {
}

func (b *UnimplementedTransformerProducerProcessor) ProcessMessage(m Message) ([]Message, BAMData, error) {
	const semLogContext = "t-prod-processor::process-message"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) AddMessage2Batch(m Message) error {
	const semLogContext = "t-prod-processor::add-to-batch"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) ProcessBatch() error {
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

type MessageOptions struct {
	Span            opentracing.Span
	HarSpan         hartracing.Span
	MessageProducer MessageProducer
}

type MessageOption func(opts *MessageOptions)

func MessageWithSpan(span opentracing.Span) MessageOption {
	return func(opts *MessageOptions) {
		opts.Span = span
	}
}

func MessageWithHarSpan(span hartracing.Span) MessageOption {
	return func(opts *MessageOptions) {
		opts.HarSpan = span
	}
}

func MessageWithProducer(mp MessageProducer) MessageOption {
	return func(opts *MessageOptions) {
		opts.MessageProducer = mp
	}
}
