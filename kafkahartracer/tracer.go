package kafkahartracer

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/har"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"strings"
	"time"
)

const (
	semLogContextBase  = "kafka-har-tracer"
	HarKafkaTracerType = "har-kafka-tracer"
	BrokerNameEnvVar   = "HAR_KAFKA_BROKER_NAME"
	TopicNameEnvVar    = "HAR_KAFKA_TOPIC_NAME"
)

type tracerImpl struct {
	done     bool
	lks      *kafkalks.LinkedService
	producer *kafka.Producer
	outCh    chan *har.HAR
	topic    string
}

type tracerOpts struct {
	brokerName string
	lks        *kafkalks.LinkedService
	topic      string
}

type Option func(opts *tracerOpts)

func WithKafkaConfig(cfg *kafkalks.Config) Option {
	return func(opts *tracerOpts) {
		var err error
		opts.lks, err = kafkalks.NewKafkaServiceInstanceWithConfig(*cfg)
		if err != nil {
			log.Error().Err(err).Msg(semLogContextBase + "::new")
		}
	}
}

func WithKafkaLinkedService(lks *kafkalks.LinkedService) Option {
	return func(opts *tracerOpts) {
		opts.lks = lks
	}
}

func WithTopic(t string) Option {
	return func(opts *tracerOpts) {
		opts.topic = t
	}
}

func NewTracer(opts ...Option) (hartracing.Tracer, io.Closer, error) {

	const semLogContext = semLogContextBase + "::new"

	trcOpts := tracerOpts{}
	for _, o := range opts {
		o(&trcOpts)
	}

	if trcOpts.lks == nil {
		err := errors.New("invalid linked service")
		log.Error().Err(err).Msg(semLogContext)
		return nil, nil, err
	}

	if trcOpts.topic == "" {
		err := errors.New("invalid topic")
		log.Error().Err(err).Str("topic", trcOpts.topic).Msg(semLogContext)
		return nil, nil, err
	}

	producer, err := trcOpts.lks.NewProducer(context.Background(), "")
	if err != nil {
		return nil, nil, err
	}

	t := &tracerImpl{producer: producer, topic: trcOpts.topic, lks: trcOpts.lks, outCh: make(chan *har.HAR, 10)}
	go t.processLoop()
	go t.monitorProducerEvents(t.producer)
	return t, t, nil
}

func (tp *tracerImpl) monitorProducerEvents(producer *kafka.Producer) {

	const semLogContext = semLogContextBase + "::events-monitor"
	log.Info().Msg(semLogContext + " starting...")

	exitFromLoop := false
	for e := range producer.Events() {

		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Info().Interface("event", ev).Msg(semLogContextBase + " delivery failed")
			} else {
				log.Trace().Interface("partition", ev.TopicPartition).Msg(semLogContextBase + " message delivered")
			}
		}

		if exitFromLoop {
			break
		}
	}

	log.Info().Msg(semLogContext + " ...ended")
}

func (t *tracerImpl) Close() error {

	const semLogContext = semLogContextBase + "::close"

	close(t.outCh)

	for t.producer.Flush(1000) != 0 {
		log.Info().Msg(semLogContext + " flushing pending messages..")
	}
	t.producer.Close()

	for !t.done {
		time.Sleep(1 * time.Second)
	}

	log.Info().Msg(semLogContext + " closed")
	return nil
}

func (t *tracerImpl) IsNil() bool {
	return false
}

func (t *tracerImpl) StartSpan(opts ...hartracing.SpanOption) hartracing.Span {
	const semLogContext = semLogContextBase + "::start-har-span"

	spanOpts := hartracing.SpanOptions{}
	for _, o := range opts {
		o(&spanOpts)
	}

	oid := util.NewObjectId().String()
	spanCtx := hartracing.SimpleSpanContext{LogId: oid, ParentId: oid, TraceId: oid, Flag: hartracing.HARSpanFlagSampled}

	if spanOpts.ParentContext != nil {
		if ctxImpl, ok := spanOpts.ParentContext.(hartracing.SimpleSpanContext); ok {
			spanCtx.LogId = ctxImpl.LogId
			spanCtx.ParentId = ctxImpl.TraceId
		} else {
			log.Warn().Msg(semLogContext + " unsupported implementation: wanted internal.spanContextImpl")
		}
	}

	span := spanImpl{
		hartracing.SimpleSpan{
			Tracer:      t,
			SpanContext: spanCtx,
			StartTime:   time.Now(),
		},
	}

	return &span
}

func (t *tracerImpl) Report(s *spanImpl) error {
	const semLogContext = semLogContextBase + "::report"

	h, err := s.GetHARData()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	t.outCh <- h
	return nil
}

func (t *tracerImpl) processLoop() error {
	const semLogContext = semLogContextBase + "::process-loop"

	log.Info().Msg(semLogContext + " starting loop...")

	for h := range t.outCh {
		msg, err := json.Marshal(h)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			continue
		}

		km := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &t.topic, Partition: kafka.PartitionAny},
			Key:            []byte(h.Log.TraceId),
			Value:          msg,
		}

		km.Headers = append(km.Headers, kafka.Header{
			Key:   hartracing.HARTraceIdHeaderName,
			Value: []byte(h.Log.TraceId),
		})

		if err := t.producer.Produce(km, nil); err != nil {
			log.Error().Err(err).Msg("errors in producing message")
		}

	}

	log.Info().Msg(semLogContext + " ending loop...")
	t.done = true
	return nil
}

func (t *tracerImpl) merge(incoming *har.HAR, fileName string) (*har.HAR, error) {

	const semLogContext = semLogContextBase + "::merge"
	log.Trace().Str("log-id", incoming.Log.TraceId).Str("fn", fileName).Msg(semLogContext)

	b, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var another har.HAR
	err = json.Unmarshal(b, &another)
	if err != nil {
		return nil, err
	}

	var mergeResult *har.HAR
	if incoming.Log.TraceId < another.Log.TraceId {
		log.Trace().Str("into-log-id", incoming.Log.TraceId).Str("from-log-id", another.Log.TraceId).Msg(semLogContext + " add file log to current log")
		mergeResult, err = incoming.Merge(&another, harEntryCompare)
	} else {
		log.Trace().Str("from-log-id", incoming.Log.TraceId).Str("into-log-id", another.Log.TraceId).Msg(semLogContext + " add current log to file log")
		mergeResult, err = another.Merge(incoming, harEntryCompare)
	}

	if err != nil {
		return nil, err
	}

	return mergeResult, nil
}

func harEntryCompare(e1, e2 *har.Entry) bool {
	return e1.TraceId < e2.TraceId
}

func (t *tracerImpl) Extract(format string, tmr hartracing.TextMapReader) (hartracing.SpanContext, error) {

	var spanContext hartracing.SimpleSpanContext
	err := tmr.ForeachKey(func(key, val string) error {
		var err error
		if strings.ToLower(key) == hartracing.HARTraceIdHeaderName {
			spanContext, err = hartracing.ExtractSimpleSpanContextFromString(val)
			return err
		}

		return nil
	})

	if spanContext.IsZero() {
		err = hartracing.ErrSpanContextNotFound
	}

	return spanContext, err
}

func (t *tracerImpl) Inject(s hartracing.SpanContext, tmr hartracing.TextMapWriter) error {
	tmr.Set(hartracing.HARTraceIdHeaderName, s.Id())
	return nil
}
