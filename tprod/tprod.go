package tprod

import (
	"context"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod/processor"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"io"
	"sync"
	"time"
)

const (
	TProdStdMetricErrors          = "_errors"
	TprodStdMetricMessages        = "_messages"
	TprodStdMetricMessagesToTopic = "_messages_to_topic"
	TprodStdMetricDuration        = "_duration"
)

type TransformerProducer interface {
	Start()
	Close()
	SetParent(s Server)
}

type transformerProducerImpl struct {
	cfg *TransformerProducerConfig

	wg           *sync.WaitGroup
	shutdownSync sync.Once
	quitc        chan struct{}
	monitorQuitc chan struct{}

	parent Server

	txActive  bool
	producers map[string]*kafka.Producer
	consumer  *kafka.Consumer

	partitionsCnt int
	eofCnt        int

	numberOfMessages int
	metrics          promutil.MetricRegistry
	processor        processor.TransformerProducerProcessor
}

const (
	DefaultPoolLoopTickInterval = 200 * time.Millisecond
)

func (tp *transformerProducerImpl) SetParent(s Server) {
	tp.parent = s
}

func (tp *transformerProducerImpl) Start() {
	const semLogContext = "t-prod::start"
	log.Info().Int("num-t-prods", len(tp.producers)).Msg(semLogContext)

	// Add to wait group
	if tp.wg != nil {
		tp.wg.Add(1)
	}

	if len(tp.producers) == 0 {
		log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " no to-topics configured.... skipping monitoring events.")
	}

	for _, p := range tp.producers {
		v := p
		go tp.monitorProducerEvents(v)
	}

	err := tp.consumer.Subscribe(tp.cfg.FromTopic.Name, nil)
	if err != nil {
		log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " topic subscription error")
		return
	}

	go tp.pollLoop()
}

func (tp *transformerProducerImpl) Close() {
	const semLogContext = "t-prod::close"
	log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " signalling shutdown transformer producer")
	close(tp.quitc)
}

func (tp *transformerProducerImpl) monitorProducerEvents(producer *kafka.Producer) {
	const semLogContext = "t-prod::monitor-producer"
	log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " starting monitor producer events")

	exitFromLoop := false
	for e := range producer.Events() {

		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Info().Interface("event", ev).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " delivery failed")
				if err := tp.abortTransaction(nil, true); err != nil {
					log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " abort transaction")
				}

				if tp.exitOnError(semLogContext, ev.TopicPartition.Error) {
					exitFromLoop = true
				}
			} else {
				log.Trace().Interface("partition", ev.TopicPartition).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " delivered message")
			}
		}

		if exitFromLoop {
			break
		}
	}

	close(tp.monitorQuitc)
	log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " exiting from monitor producer events")
}

func (tp *transformerProducerImpl) exitOnError(semLogContext string, err error) bool {

	if err != nil {
		if err == io.EOF {
			if tp.cfg.Exit.OnEof {
				log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " exiting on eof reached")
				return true
			}
		} else {
			if tp.cfg.Exit.OnFail {
				log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " exiting on error")
				return true
			}
		}
	}

	return false
}

func (tp *transformerProducerImpl) pollLoop() {
	const semLogContext = "t-prod::poll-loop"
	log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " starting polling loop")

	ticker := time.NewTicker(tp.cfg.TickInterval)

	for {
		select {
		case <-ticker.C:
			// _ = level.Info(tp.logger).Log(system.DefaultLogMessageField, "I'm ticking")
			if err := tp.processBatch(context.Background()); err != nil && tp.cfg.Exit.OnFail {
				ticker.Stop()
				tp.shutDown(err)
				return
			}
		case <-tp.quitc:
			log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " terminating poll loop")
			ticker.Stop()
			tp.shutDown(nil)
			return

		case <-tp.monitorQuitc:
			log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " monitor quitted")
			ticker.Stop()
			tp.shutDown(errors.New(tp.cfg.Name + " monitor producer error"))
			return

		default:
			if isMsg, err := tp.poll(); err != nil {
				if tp.exitOnError(semLogContext, err) {
					ticker.Stop()
					tp.shutDown(err)
				}

				return
			} else if isMsg {
				tp.numberOfMessages++
				if tp.cfg.Exit.EofAfterN > 0 && tp.numberOfMessages >= tp.cfg.Exit.EofAfterN {
					if tp.cfg.Exit.OnEof {
						log.Info().Int("number-of-messages", tp.numberOfMessages).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " poll max number of events reached, transform producer exiting....")
						tp.Close()
					}
				}
			}
		}
	}
}

func (tp *transformerProducerImpl) processBatch(ctx context.Context) error {
	return nil
}

func (tp *transformerProducerImpl) shutDown(err error) {
	const semLogContext = "t-prod::shutdown"

	tp.shutdownSync.Do(func() {
		if tp.wg != nil {
			tp.wg.Done()
		}

		for _, p := range tp.producers {
			p.Close()
		}
		tp.producers = nil

		if tp.consumer != nil {
			_ = tp.consumer.Close()
		}
		tp.consumer = nil

		if tp.parent != nil {
			tp.parent.TransformerProducerTerminated(err)
		} else {
			log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " parent has not been set....")
		}
	})

}

func (tp *transformerProducerImpl) poll() (bool, error) {
	const semLogContext = "t-prod::poll"
	var err error

	isMessage := false
	ev := tp.consumer.Poll(tp.cfg.FromTopic.MaxPollTimeout)
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		log.Info().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " assigned partitions")

		if err = tp.consumer.Assign(e.Partitions); err != nil {
			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " assigned partitions")
		}
		tp.partitionsCnt = len(e.Partitions)
		tp.eofCnt = 0
	case kafka.RevokedPartitions:
		log.Info().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " revoked partitions")
		if err = tp.consumer.Unassign(); err != nil {
			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " revoked partitions")
		}

		if tp.txActive {
			_ = tp.abortTransaction(nil, false)
		}

		tp.partitionsCnt = 0
		tp.eofCnt = 0
	case *kafka.Message:

		isMessage = true
		beginOfProcessing := time.Now()
		sysMetricInfo := processor.BAMData{}
		sysMetricInfo.AddMessageHeaders(e.Headers)
		span, harSpan := tp.requestSpans(e.Headers)
		defer span.Finish()
		if harSpan != nil {
			defer harSpan.Finish()
		}

		if err = tp.beginTransaction(false); err != nil {
			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error beginning transaction")
			tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo)
			return isMessage, err
		}

		msg, bamData, err := tp.processor.Process(e, processor.TransformerProducerProcessorWithSpan(span), processor.TransformerProducerProcessorWithHarSpan(harSpan))
		if err != nil {
			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error processing message")
			_ = tp.abortTransaction(context.Background(), true)
			tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo.AddBAMData(bamData))
			return isMessage, err
		}

		err = tp.produce2Topic(msg)
		if err != nil {
			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error producing output message")
			_ = tp.abortTransaction(context.Background(), true)
			tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo.AddBAMData(bamData))
			return isMessage, err
		}

		err = tp.commitTransaction(context.Background(), true)
		if err != nil {
			tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo.AddBAMData(bamData))
			return isMessage, err
		}

		tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo.AddBAMData(bamData))

	case kafka.PartitionEOF:
		log.Info().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " eof partition reached")
		tp.eofCnt++
		if tp.cfg.Exit.OnEof && tp.eofCnt >= tp.partitionsCnt {
			err = io.EOF
		}
	case kafka.Error:
		// Errors should generally be considered as informational, the client will try to automatically recover
		log.Error().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " errors received")
	case kafka.OffsetsCommitted:
		log.Info().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " committed offsets")
	default:
		break
	}

	return isMessage, err
}

func (tp *transformerProducerImpl) beginTransaction(warnOnRunning bool) error {
	const semLogContext = "t-prod::begin-transaction"
	log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Bool("tx-active", tp.txActive).Bool("enabled", kafkalks.IsTransactionCommit(tp.cfg.CommitMode)).Msg(semLogContext)
	if !kafkalks.IsTransactionCommit(tp.cfg.CommitMode) {
		return nil
	}

	if tp.txActive {
		if warnOnRunning {
			log.Warn().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " transaction already running...")
		}
	} else {
		if err := tp.getProducer().BeginTransaction(); err != nil {
			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " begin transaction error")
			return err
		} else {
			tp.txActive = true
		}
	}

	return nil
}

func (tp *transformerProducerImpl) abortTransaction(ctx context.Context, warnOnNotRunning bool) error {
	const semLogContext = "t-prod::abort-transaction"
	log.Warn().Str(semLogTransformerProducerId, tp.cfg.Name).Bool("tx-active", tp.txActive).Bool("enabled", kafkalks.IsTransactionCommit(tp.cfg.CommitMode)).Msg(semLogContext + " aborting transaction...")

	switch tp.cfg.CommitMode {
	case kafkalks.CommitModeAuto:
		return nil
	case kafkalks.CommitModeManual:
		log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " no  action on aborting on manual commit...")

	case kafkalks.CommitModeTransaction:
		if tp.txActive {
			if err := tp.getProducer().AbortTransaction(ctx); err != nil {
				log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " abort transaction error")
				return err
			}
		} else {
			if warnOnNotRunning {
				log.Warn().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " transaction not active")
			}
		}
		tp.txActive = false
	default:
		log.Warn().Str("commit-mode", tp.cfg.CommitMode).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " commit-mode not recognized")
	}

	return nil
}

func (tp *transformerProducerImpl) commitTransaction(ctx context.Context, warnOnNotRunning bool) error {
	const semLogContext = "t-prod::commit-transaction"
	log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Bool("tx-active", tp.txActive).Bool("enabled", kafkalks.IsTransactionCommit(tp.cfg.CommitMode)).Msg(semLogContext)

	switch tp.cfg.CommitMode {
	case kafkalks.CommitModeAuto:
		return nil
	case kafkalks.CommitModeManual:
		_, err := tp.consumer.Commit()
		if err != nil {
			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error on commit message")
			return err
		}
	case kafkalks.CommitModeTransaction:
		if tp.txActive {

			partitions, err := tp.consumer.Assignment()
			if err != nil {
				log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer assignment error")
				return err
			}

			positions, err := tp.consumer.Position(partitions)
			if err != nil {
				log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer position error")
				return err
			}

			consumerMetadata, err := tp.consumer.GetConsumerGroupMetadata()
			if err != nil {
				log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer get group metadata")
				return err
			}

			//fmt.Fprintln(os.Stdout, "ConsumerMetaData: ", consumerMetadata)
			err = tp.getProducer().SendOffsetsToTransaction(ctx, positions, consumerMetadata)
			if err != nil {
				log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer send offset to transaction")
				return err
			}

			err = tp.getProducer().CommitTransaction(ctx)
			tp.txActive = false
			if err != nil {
				log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer commit transaction")
				return err
			}
		} else {
			if warnOnNotRunning {
				log.Warn().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " transaction not active")
			}
		}
	default:
		log.Warn().Str(semLogTransformerProducerId, tp.cfg.Name).Str("commit-mode", tp.cfg.CommitMode).Msg(semLogContext + " commit-mode not recognized")
	}

	return nil
}

func (tp *transformerProducerImpl) getProducerForTopic(topicCfg *ConfigTopic) (*kafka.Producer, error) {
	n := util.Coalesce(topicCfg.BrokerName, tp.cfg.BrokerName)
	if p, ok := tp.producers[n]; ok {
		return p, nil
	}

	return nil, fmt.Errorf("cannot find producer for topic %s in broker %s", topicCfg.Name, n)
}

func (tp *transformerProducerImpl) getProducer() *kafka.Producer {

	if len(tp.producers) == 1 {
		for _, p := range tp.producers {
			return p
		}
	}

	panic(fmt.Errorf("ambiguous get of first producer out of %d", len(tp.producers)))
}

func (tp *transformerProducerImpl) produce2Topic(m processor.Message) error {
	const semLogContext = "t-prod::produce-to-topic"

	if m.IsZero() {
		if tp.cfg.CountTopicsByType("std") > 0 {
			// Produce a warn only in case there are standard topics configured
			log.Warn().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " message empty no output provided")
		}
		return nil
	}

	if len(tp.cfg.ToTopics) == 0 {
		return nil
	}

	if tcfg, err := tp.cfg.FindTopicByType(m.TopicType); err != nil {
		log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Str("type", string(m.TopicType)).Msg(semLogContext + " error in determining target topic")
		return err
	} else {
		log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Str("topic", tp.cfg.ToTopics[tcfg].Name).Msg("producing message")

		headers := make(map[string]string)
		if m.Span != nil {
			opentracing.GlobalTracer().Inject(
				m.Span.Context(),
				opentracing.TextMap,
				opentracing.TextMapCarrier(headers))
		} else {
			log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " message trace has not been set")
		}

		for headerKey, headerValue := range m.Headers {
			headers[headerKey] = headerValue
		}

		var kh []kafka.Header
		for headerKey, headerValue := range headers {
			kh = append(kh, kafka.Header{
				Key:   headerKey,
				Value: []byte(headerValue),
			})
		}

		km := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &tp.cfg.ToTopics[tcfg].Name, Partition: kafka.PartitionAny},
			Key:            m.Key,
			Value:          m.Body,
			Headers:        kh,
		}

		producer, err := tp.getProducerForTopic(&tp.cfg.ToTopics[tcfg])
		if err != nil {
			return err
		}

		if err := producer.Produce(km, nil); err != nil {
			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " errors in producing message")
			return err
		}
	}

	return nil
}

func (tp *transformerProducerImpl) produceMetrics(elapsed float64, err error, data processor.BAMData) {

	const semLogContext = "t-prod::produce-metrics"
	log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Float64("elapsed", elapsed).Msg(semLogContext)

	// data.Trace()

	tp.metrics.SetMetricValueById(TprodStdMetricMessages, 1, data.Labels)
	tp.metrics.SetMetricValueById(TprodStdMetricDuration, elapsed, data.Labels)
	if err != nil {
		tp.metrics.SetMetricValueById(TProdStdMetricErrors, 1, data.Labels)
	}
	for _, md := range data.MetricsData {
		_ = tp.metrics.SetMetricValueById(md.MetricId, md.Value, data.Labels)
	}

}

func (tp *transformerProducerImpl) requestSpans(hs []kafka.Header) (opentracing.Span, hartracing.Span) {

	const semLogContext = "t-prod::request-span"

	spanName := tp.cfg.Tracing.SpanName

	if spanName == "" {
		spanName = tp.cfg.Name
	}

	if spanName == "" {
		spanName = "not-assigned"
		log.Warn().Msgf(semLogContext+" span name cannot be assigned...defaulting to '%s'", spanName)
	}

	headers := make(map[string]string)
	for _, header := range hs {
		headers[header.Key] = string(header.Value)
	}

	spanContext, _ := opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapCarrier(headers))
	log.Trace().Bool("span-from-message", spanContext != nil).Msg(semLogContext)

	var span opentracing.Span
	if spanContext != nil {
		span = opentracing.StartSpan(spanName, opentracing.FollowsFrom(spanContext))
	} else {
		span = opentracing.StartSpan(spanName)
	}

	harSpanContext, harSpanErr := hartracing.GlobalTracer().Extract("", hartracing.TextMapCarrier(headers))
	log.Trace().Bool("har-span-from-message", harSpanErr == nil).Msg(semLogContext)

	var harSpan hartracing.Span
	if harSpanErr == nil {
		harSpan = hartracing.GlobalTracer().StartSpan(hartracing.ChildOf(harSpanContext))
	}

	return span, harSpan
}
