package tprod

import (
	"context"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"io"
	"sync"
	"time"
)

const (
	MetricBatchErrors     = "tprod-batch-errors"
	MetricBatches         = "tprod-batches"
	MetricBatchSize       = "tprod-batch-size"
	MetricBatchDuration   = "tprod-batch-duration"
	MetricMessageErrors   = "tprod-event-errors"
	MetricMessages        = "tprod-events"
	MetricMessagesToTopic = "tprod-events-to-topic"
	MetricMessageDuration = "tprod-event-duration"
)

type TransformerProducer interface {
	Start()
	Close()
	SetParent(s Server)
	Name() string
}

type TransformerProducerError struct {
	Level string
	Err   error
}

func (e *TransformerProducerError) Error() string { return e.Level + ": " + e.Err.Error() }

type transformerProducerImpl struct {
	cfg *TransformerProducerConfig

	wg           *sync.WaitGroup
	shutdownSync sync.Once
	quitc        chan struct{}
	monitorQuitc chan struct{}

	parent Server

	txActive    bool
	producers   map[string]*kafka.Producer
	msgProducer MessageProducer
	consumer    *kafka.Consumer

	partitionsCnt int
	eofCnt        int

	numberOfMessages int
	processor        TransformerProducerProcessor

	metricLabels map[string]string
}

const (
	DefaultPoolLoopTickInterval = 200 * time.Millisecond
)

func (tp *transformerProducerImpl) Name() string {
	return tp.cfg.Name
}

func (tp *transformerProducerImpl) SetParent(s Server) {
	tp.parent = s
}

func (tp *transformerProducerImpl) Start() {
	const semLogContext = "t-prod::start"

	if tp.cfg.StartDelay > 0 {
		time.Sleep(time.Millisecond * time.Duration(tp.cfg.StartDelay))
	}

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
	lbls := map[string]string{
		"name":        tp.cfg.Name,
		"status-code": "500",
	}
	for e := range producer.Events() {

		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Error().Err(ev.TopicPartition.Error).Int64("offset", int64(ev.TopicPartition.Offset)).Int32("partition", ev.TopicPartition.Partition).Interface("topic", ev.TopicPartition.Topic).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " delivery failed")
				lbls["status-code"] = "500"
				lbls["topic-name"] = *ev.TopicPartition.Topic
				_ = tp.produceMetric(nil, MetricMessagesToTopic, 1, lbls)
				if err := tp.abortTransaction(nil, true); err != nil {
					log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " abort transaction")
				}

				if tp.exitOnError(semLogContext, ev.TopicPartition.Error) {
					exitFromLoop = true
				}
			} else {
				lbls["status-code"] = "200"
				lbls["topic-name"] = *ev.TopicPartition.Topic
				tp.produceMetric(nil, MetricMessagesToTopic, 1, lbls)
				log.Trace().Int64("offset", int64(ev.TopicPartition.Offset)).Int32("partition", ev.TopicPartition.Partition).Interface("topic", ev.TopicPartition.Topic).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " delivery ok")
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
			if tp.cfg.OnEof == OnEofExit {
				log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " exiting on eof reached")
				return true
			}
		} else {
			if tp.cfg.ErrorPolicyForError(err) == OnErrorExit {
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
			if tp.cfg.WorkMode == WorkModeBatch {
				beginOfProcessing := time.Now()
				batchSize := tp.processor.BatchSize()
				err := tp.processBatch(context.Background())
				if batchSize > 0 {
					metricGroup := tp.produceMetric(nil, MetricBatches, 1, tp.metricLabels)
					metricGroup = tp.produceMetric(metricGroup, MetricBatchSize, float64(batchSize), tp.metricLabels)
					metricGroup = tp.produceMetric(metricGroup, MetricBatchDuration, time.Since(beginOfProcessing).Seconds(), tp.metricLabels)
				}
				if err != nil && tp.cfg.ErrorPolicyForError(err) == OnErrorExit {
					_ = tp.produceMetric(nil, MetricBatchErrors, 1, tp.metricLabels)
					ticker.Stop()
					tp.shutDown(err)
					return
				}
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
					return
				}
			} else if isMsg {
				tp.numberOfMessages++
				if tp.cfg.EofAfterN > 0 && tp.numberOfMessages >= tp.cfg.EofAfterN {
					if tp.cfg.OnEof == OnEofExit {
						log.Info().Int("number-of-messages", tp.numberOfMessages).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " poll max number of events reached, transform producer exiting....")
						tp.Close()
					}
				}
			}
		}
	}
}

func (tp *transformerProducerImpl) addMessage2Batch(km *kafka.Message) error {
	const semLogContext = "t-prod::add-message-2-batch"
	var err error
	if err = tp.beginTransaction(false); err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	spanName := tp.cfg.Tracing.SpanName
	if spanName == "" {
		spanName = tp.cfg.Name
	}
	msgIn := NewMessage(spanName, km, MessageWithProducer(tp.msgProducer))
	err = tp.processor.AddMessage2Batch(msgIn)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return nil
}

func (tp *transformerProducerImpl) processBatch(ctx context.Context) error {
	const semLogContext = "t-prod::process-batch"

	if tp.cfg.WorkMode != WorkModeBatch {
		return nil
	}

	defer tp.msgProducer.Release()

	if tp.processor.BatchSize() == 0 {
		err := tp.msgProducer.Close()
		if err != nil {
			_ = tp.abortTransaction(context.Background(), false)
			return err
		}

		if tp.txActive {
			log.Info().Msg(semLogContext + " - transaction is active but batch is of size zero... committing")
			if err := tp.commitTransaction(ctx, false); err != nil {
				_ = tp.abortTransaction(context.Background(), false)
				return err
			}
		}
		return nil
	}

	if err := tp.processor.ProcessBatch(); err != nil {
		_ = tp.abortTransaction(context.Background(), false)
		return err
	} else {
		err := tp.msgProducer.Close()
		if err != nil {
			_ = tp.abortTransaction(context.Background(), false)
			return err
		}
	}

	tp.processor.Clear()

	if err := tp.commitTransaction(ctx, false); err != nil {
		_ = tp.abortTransaction(context.Background(), false)
		return err
	}

	return nil
}

func (tp *transformerProducerImpl) processMessage(e *kafka.Message) (BAMData, error) {
	const semLogContext = "t-prod::process-message"

	var err error

	beginOfProcessing := time.Now()
	sysMetricInfo := BAMData{}
	sysMetricInfo.AddMessageHeaders(e.Headers)
	sysMetricInfo.AddLabels(tp.metricLabels)

	/*
		span, harSpan := tp.requestSpans(e.Headers)
		defer span.Finish()
		if harSpan != nil {
			defer harSpan.Finish()
		}
	*/
	spanName := tp.cfg.Tracing.SpanName
	if spanName == "" {
		spanName = tp.cfg.Name
	}
	msgIn := NewMessage(spanName, e)
	defer msgIn.Finish()

	if err = tp.beginTransaction(false); err != nil {
		log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error beginning transaction")
		tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo)
		return sysMetricInfo, err
	}

	msg, bamData, procErr := tp.processor.ProcessMessage(msgIn)
	sysMetricInfo.AddBAMData(bamData)
	if procErr != nil {
		log.Error().Err(procErr).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error processing message")
		switch tp.cfg.ErrorPolicyForError(err) {
		case OnErrorDeadLetter:
			msg = []Message{{Span: msgIn.Span,
				ToTopic: TargetTopic{TopicType: TopicTypeDeadLetter},
				Headers: ToMessageHeaders(e.Headers),
				Key:     e.Key,
				Body:    e.Value,
			}}
		default:
			_ = tp.abortTransaction(context.Background(), true)
			tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), procErr, sysMetricInfo.AddBAMData(bamData))
			return sysMetricInfo, procErr
		}
	}

	err = tp.produce2Topic(msg)
	if err != nil {
		log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error producing output message")
		_ = tp.abortTransaction(context.Background(), true)
		tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo.AddBAMData(bamData))
		return sysMetricInfo, err
	}

	err = tp.commitTransaction(context.Background(), true)
	if err != nil {
		tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo.AddBAMData(bamData))
		return sysMetricInfo, err
	}

	tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), util.CoalesceError(procErr, err), sysMetricInfo.AddBAMData(bamData))
	return sysMetricInfo, nil
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

		beginOfProcessing := time.Now()

		isMessage = true

		var metricGroup *promutil.Group
		if tp.cfg.WorkMode == WorkModeBatch {
			err = tp.addMessage2Batch(e)
			metricGroup = tp.produceMetric(metricGroup, MetricMessages, 1, tp.metricLabels)
		} else {
			var bamData BAMData
			bamData, err = tp.processMessage(e)

			metricGroup = tp.produceMetric(metricGroup, MetricMessages, 1, bamData.Labels)
			metricGroup = tp.produceMetric(metricGroup, MetricMessageDuration, time.Since(beginOfProcessing).Seconds(), bamData.Labels)
		}

		if err != nil {
			metricGroup = tp.produceMetric(metricGroup, MetricMessageErrors, 1, tp.metricLabels)
		}

		/*
			beginOfProcessing := time.Now()
			sysMetricInfo := BAMData{}
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

			msg, bamData, procErr := tp.processor.Process(e, TransformerProducerProcessorWithSpan(span), TransformerProducerProcessorWithHarSpan(harSpan))
			if procErr != nil {
				log.Error().Err(procErr).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error processing message")
				switch tp.cfg.OnError {
				case OnErrorDeadLetter:
					msg = []Message{{Span: span,
						ToTopic: TargetTopic{TopicType: TopicTypeDeadLetter},
						Headers: ToMessageHeaders(e.Headers),
						Key:     e.Key,
						Body:    e.Value,
					}}
				default:
					_ = tp.abortTransaction(context.Background(), true)
					tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), procErr, sysMetricInfo.AddBAMData(bamData))
					return isMessage, procErr
				}
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

			tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), util.CoalesceError(procErr, err), sysMetricInfo.AddBAMData(bamData))
		*/
	case kafka.PartitionEOF:
		log.Info().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " eof partition reached")
		tp.eofCnt++
		if tp.cfg.OnEof == OnEofExit && tp.eofCnt >= tp.partitionsCnt {
			err = io.EOF
		}
	case kafka.Error:
		// Errors should generally be considered as informational, the client will try to automatically recover
		log.Info().Str("error", e.Error()).Int("err-code", int(e.Code())).Bool("tx-requires-abort", e.TxnRequiresAbort()).Bool("is-retriable", e.IsRetriable()).Bool("is-fatal", e.IsFatal()).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " errors received")
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
	n := util.StringCoalesce(topicCfg.BrokerName, tp.cfg.BrokerName)
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

func (tp *transformerProducerImpl) produce2Topic(msgs []Message) error {
	const semLogContext = "t-prod::produce-to-topic"

	if len(msgs) == 0 {
		if tp.cfg.CountTopicsByType("std") > 0 {
			// Produce a warn only in case there are standard topics configured
			log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " message empty no output provided")
		}
		return nil
	}

	if len(tp.cfg.ToTopics) == 0 {
		return nil
	}

	for _, m := range msgs {
		if tcfg, err := tp.cfg.FindTopicByIdOrType(m.ToTopic.Id, m.ToTopic.TopicType); err != nil {
			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Str("type", string(m.ToTopic.TopicType)).Msg(semLogContext + " error in determining target topic")
			return err
		} else {
			log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Bool("muted", tp.cfg.ToTopics[tcfg].MuteOn).Str("topic", tp.cfg.ToTopics[tcfg].Name).Msg("producing message")

			if !tp.cfg.ToTopics[tcfg].MuteOn {
				headers := make(map[string]string)
				if m.Span != nil {
					opentracing.GlobalTracer().Inject(
						m.Span.Context(),
						opentracing.TextMap,
						opentracing.TextMapCarrier(headers))
				} else {
					log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " message trace has not been set")
				}

				if m.HarSpan != nil {
					hartracing.GlobalTracer().Inject(
						m.HarSpan.Context(),
						hartracing.TextMapCarrier(headers))
				} else {
					log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " message har-trace has not been set")
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

		}
	}

	return nil
}

func (tp *transformerProducerImpl) produceMetric(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
	const semLogContext = "t-prod::produce-metric"

	var err error
	if metricGroup == nil {
		g, err := promutil.GetGroup(tp.cfg.RefMetrics.GId)
		if err != nil {
			log.Warn().Err(err).Msg(semLogContext)
			return nil
		}

		metricGroup = &g
	}

	err = metricGroup.SetMetricValueById(metricId, value, labels)
	if err != nil {
		log.Warn().Err(err).Msg(semLogContext)
	}

	return metricGroup
}

func (tp *transformerProducerImpl) produceMetrics(elapsed float64, errParam error, data BAMData) {

	const semLogContext = "t-prod::produce-metrics"
	log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Float64("elapsed", elapsed).Msg(semLogContext + "...disabled")

	/*
		if tp.cfg.RefMetrics != nil && tp.cfg.RefMetrics.IsEnabled() {
			g, err := promutil.GetGroup(tp.cfg.RefMetrics.GId)
			if err != nil {
				log.Warn().Err(err).Msg(semLogContext)
				return
			}

			err = g.SetMetricValueById(MetricMessages, 1, data.Labels)
			if err != nil {
				log.Warn().Err(err).Msg(semLogContext)
			}

			err = g.SetMetricValueById(MetricMessageDuration, elapsed, data.Labels)
			if err != nil {
				log.Warn().Err(err).Msg(semLogContext)
			}

			if errParam != nil {
				err = g.SetMetricValueById(MetricMessageErrors, 1, data.Labels)
				if err != nil {
					log.Warn().Err(err).Msg(semLogContext)
					return
				}
			}

			for _, md := range data.MetricsData {
				err = g.SetMetricValueById(md.MetricId, md.Value, data.Labels)
				if err != nil {
					log.Warn().Err(err).Msg(semLogContext)
					return
				}
			}
		}
	*/
}
