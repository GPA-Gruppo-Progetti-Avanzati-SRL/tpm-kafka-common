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
	MetricBatchErrors         = "tprod-batch-errors"
	MetricBatches             = "tprod-batches"
	MetricBatchSize           = "tprod-batch-size"
	MetricBatchDuration       = "tprod-batch-duration"
	MetricMessageErrors       = "tprod-event-errors"
	MetricMessages            = "tprod-events"
	MetricMessagesToTopic     = "tprod-events-to-topic"
	MetricMessageDuration     = "tprod-event-duration"
	MetricsPartitionsEvents   = "tprod-partitions-events"
	MetricsNumberOfPartitions = "tprod-num-partitions"
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

	wg               *sync.WaitGroup
	shutdownSync     sync.Once
	quitc            chan struct{}
	monitorQuitc     chan struct{}
	shuttingDownFlag bool
	parent           Server

	brokers   []string
	producers map[string]*KafkaProducerWrapper

	msgProducer MessageProducer

	consumer           *kafka.Consumer
	consumerBrokerName string

	partitionsCnt int
	eofCnt        int

	numberOfMessages int
	processor        TransformerProducerProcessor

	metricLabels map[string]string
}

const (
	DefaultPoolLoopTickInterval = 200 * time.Millisecond
)

func (tp *transformerProducerImpl) consumerLinkedServiceConfig() (kafkalks.Config, error) {
	const semLogContext = "t-prod::get-kafka-linked-service-config"
	lks, err := kafkalks.GetKafkaLinkedService(tp.consumerBrokerName)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return kafkalks.Config{}, err
	}

	return lks.Config(), nil
}

func (tp *transformerProducerImpl) Name() string {
	return tp.cfg.Name
}

func (tp *transformerProducerImpl) SetParent(s Server) {
	tp.parent = s
}

func (tp *transformerProducerImpl) createProducers(onError bool) error {
	const semLogContext = "t-prod::create-producers"

	if onError {
		log.Error().Bool("on-error", onError).Msg(semLogContext)
	}

	//if onError {
	//	return nil
	//}

	ctx, ctxCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ctxCancel()

	for _, brokerName := range tp.brokers {

		if current, ok := tp.producers[brokerName]; ok {
			current.Close()
		}

		kp, err := NewKafkaProducerWrapper(ctx, brokerName, tp.cfg.ProducerId, kafka.TopicPartition{Partition: kafka.PartitionAny}, tp.cfg.WithSynchDelivery(), true)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}
		tp.producers[brokerName] = kp
		/*
			p, err := kafkalks.NewKafkaProducer(ctx, brokerName, t.cfg.ProducerId, kafka.TopicPartition{Partition: kafka.PartitionAny})
			if err != nil {
				return nil, err
			}

			var deliveryChannel chan kafka.Event
			if cfg.WithSynchDelivery() {
				deliveryChannel = make(chan kafka.Event)
			}
			kp := NewKafkaProducerWrapper(cfg.Name, p, deliveryChannel)
			t.producers[brokerName] = kp
		*/
		if tp.cfg.WorkMode == WorkModeBatch {
			tp.msgProducer = NewMessageProducer(tp.cfg.Name, kp, tp.cfg.KafkaQueueBufferSize, tp.cfg.ToTopics, tp.cfg.RefMetrics.GId)
		}
	}

	if !tp.cfg.WithSynchDelivery() {
		for _, p := range tp.producers {
			v := p
			go tp.monitorProducerEvents(v)
		}
	}

	return nil
}

func (tp *transformerProducerImpl) Start() {
	const semLogContext = "t-prod::start"
	var err error

	if tp.cfg.StartDelay > 0 {
		time.Sleep(time.Millisecond * time.Duration(tp.cfg.StartDelay))
	}

	log.Info().Int("num-t-prods", len(tp.producers)).Msg(semLogContext)

	// Add to wait group
	if tp.wg != nil {
		tp.wg.Add(1)
	}

	err = tp.createProducers(false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext + " creating producers failed")
		return
	}

	if len(tp.producers) == 0 {
		log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " no to-topics configured.... skipping monitoring events.")
	}

	/*
		if !tp.cfg.WithSynchDelivery() {
			for _, p := range tp.producers {
				v := p
				go tp.monitorProducerEvents(v)
			}
		}
	*/

	err = tp.consumer.Subscribe(tp.cfg.FromTopic.Name, tp.rebalanceCb)
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

func (tp *transformerProducerImpl) monitorProducerEvents(producer *KafkaProducerWrapper) {
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
				logKafkaError(ev.TopicPartition.Error).Int64("offset", int64(ev.TopicPartition.Offset)).Int32("partition", ev.TopicPartition.Partition).Interface("topic", ev.TopicPartition.Topic).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " delivery failed")
				lbls["status-code"] = "500"
				lbls["topic-name"] = *ev.TopicPartition.Topic
				_ = tp.produceMetric(nil, MetricMessagesToTopic, 1, lbls)
				if !tp.cfg.NoAbortOnAsyncDeliveryFailed() {
					if err := tp.abortTransaction(nil); err != nil {
						logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " abort transaction")
					}
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

	if tp.shuttingDownFlag {
		log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " exiting from monitor producer events and closing the monitorQuitc")
		close(tp.monitorQuitc)
	} else {
		log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " exiting from monitor producer events WOUT closing the monitorQuitc")
	}

}

func (tp *transformerProducerImpl) exitOnError(semLogContext string, err error) bool {

	if err != nil {
		if err == io.EOF {
			if tp.cfg.OnEof == OnEofExit {
				log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " exiting on eof reached")
				return true
			}
		} else {
			if ErrorPolicyForError(err, tp.cfg.OnErrors) == OnErrorExit {
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
				metricGroup := tp.produceMetric(nil, MetricBatchSize, float64(batchSize), tp.metricLabels)
				err := tp.processBatch(context.Background())
				if batchSize > 0 {
					metricGroup = tp.produceMetric(metricGroup, MetricBatches, 1, tp.metricLabels)
					metricGroup = tp.produceMetric(metricGroup, MetricBatchDuration, time.Since(beginOfProcessing).Seconds(), tp.metricLabels)
				}
				if err != nil {
					logKafkaError(err).Msg(semLogContext)
					_ = tp.produceMetric(nil, MetricBatchErrors, 1, tp.metricLabels)
					if ErrorPolicyForError(err, tp.cfg.OnErrors) == OnErrorExit {
						ticker.Stop()
						tp.shutDown(err)
						return
					} else {
						if isKafkaErrorFatal(err) {
							log.Error().Msg(semLogContext + " - error is fatal.... recreating producers")
							err = tp.createProducers(true)
							if err != nil {
								log.Error().Err(err).Msg(semLogContext)
							}
						}
					}
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
				logKafkaError(err).Msg(semLogContext)
				if tp.exitOnError(semLogContext, err) {
					ticker.Stop()
					tp.shutDown(err)
					return
				} else {
					if isKafkaErrorFatal(err) {
						log.Error().Msg(semLogContext + " - error is fatal.... recreating producers")
						err = tp.createProducers(true)
						if err != nil {
							log.Error().Err(err).Msg(semLogContext)
						}
					}
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
		logKafkaError(err).Msg(semLogContext)
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

	if tp.cfg.WorkMode != WorkModeBatch || tp.processor.BatchSize() == 0 {
		return nil
	}

	defer tp.msgProducer.Release()
	defer tp.processor.Clear()

	if tp.processor.BatchSize() == 0 {
		err := tp.msgProducer.Close()
		if err != nil {
			logKafkaError(err).Msg(semLogContext)
			abortErr := tp.abortTransaction(context.Background())
			if abortErr != nil {
				logKafkaError(abortErr).Msg(semLogContext)
				err = abortErr
			}
			return err
		}

		if err := tp.commitTransaction(ctx); err != nil {
			logKafkaError(err).Msg(semLogContext)
			/*
				abortErr := tp.abortTransaction(context.Background(), false)
				if abortErr != nil {
					logKafkaError(abortErr).Msg(semLogContext)
				}
				return util.CoalesceError(abortErr, err)
			*/
			return err
		}
		return nil
	}

	if err := tp.processor.ProcessBatch(); err != nil {
		logKafkaError(err).Msg(semLogContext)
		var abortErr error
		if KafkaErrorRequiresAbort(err, true) {
			abortErr = tp.abortTransaction(context.Background())
			if abortErr != nil {
				logKafkaError(abortErr).Msg(semLogContext)
			}
		}
		return util.CoalesceError(abortErr, err)
	} else {
		err := tp.msgProducer.Close()
		if err != nil {
			logKafkaError(err).Msg(semLogContext)
			abortErr := tp.abortTransaction(context.Background())
			if abortErr != nil {
				logKafkaError(abortErr).Msg(semLogContext)
			}
			return util.CoalesceError(abortErr, err)
		}
	}

	tp.processor.Clear()

	if err := tp.commitTransaction(ctx); err != nil {
		logKafkaError(err).Msg(semLogContext)
		//abortErr := tp.abortTransaction(context.Background(), false)
		//if abortErr != nil {
		//	logKafkaError(abortErr).Msg(semLogContext)
		//}
		//return util.CoalesceError(abortErr, err)
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
		logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error beginning transaction")
		tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo)
		return sysMetricInfo, err
	}

	msg, bamData, procErr := tp.processor.ProcessMessage(msgIn)
	sysMetricInfo.AddBAMData(bamData)
	if procErr != nil {
		logKafkaError(procErr).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error processing message")
		switch ErrorPolicyForError(err, tp.cfg.OnErrors) {
		case OnErrorDeadLetter:
			// Try to figure out if the process returned a message to be put in dlt. If not, proceed with the original one.
			var dltMsg Message
			var ok bool
			if msg != nil {
				dltMsg, ok = Messages(msg).GetDltMessage()
			}

			if !ok {
				dltMsg = msgIn
			}
			msg = []Message{{Span: dltMsg.Span,
				ToTopic: TargetTopic{TopicType: TopicTypeDeadLetter},
				Headers: dltMsg.Headers,
				Key:     e.Key,
				Body:    e.Value,
			}}
		default:
			tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), procErr, sysMetricInfo.AddBAMData(bamData))
			abortErr := tp.abortTransaction(context.Background())
			if abortErr != nil {
				logKafkaError(abortErr).Msg(semLogContext)
			}
			return sysMetricInfo, util.CoalesceError(abortErr, procErr)
		}
	}

	err = tp.produce2Topic(msg)
	if err != nil {
		logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " error producing output message")
		abortErr := tp.abortTransaction(context.Background())
		if abortErr != nil {
			logKafkaError(abortErr).Msg(semLogContext)
		}
		tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo.AddBAMData(bamData))
		return sysMetricInfo, util.CoalesceError(abortErr, err)
	}

	err = tp.commitTransaction(context.Background())
	if err != nil {
		logKafkaError(err).Msg(semLogContext)
		tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), err, sysMetricInfo.AddBAMData(bamData))
		return sysMetricInfo, err
	}

	tp.produceMetrics(time.Since(beginOfProcessing).Seconds(), util.CoalesceError(procErr, err), sysMetricInfo.AddBAMData(bamData))
	return sysMetricInfo, nil
}

func (tp *transformerProducerImpl) shutDown(err error) {
	const semLogContext = "t-prod::shutdown"

	tp.shutdownSync.Do(func() {
		tp.shuttingDownFlag = true

		if tp.wg != nil {
			tp.wg.Done()
		}

		if tp.consumer != nil {
			_ = tp.consumer.Close()
		}
		tp.consumer = nil

		for _, p := range tp.producers {
			p.Close()
		}
		tp.producers = nil

		if tp.parent != nil {
			tp.parent.TransformerProducerTerminated(err)
		} else {
			log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " parent has not been set....")
		}
	})

}

func (tp *transformerProducerImpl) rebalanceCb(c *kafka.Consumer, ev kafka.Event) error {
	const semLogContext = "t-prod::rebalance-callback"

	cfg, err := tp.consumerLinkedServiceConfig()
	if err != nil {
		return err
	}

	partitionAssignmentStrategy := cfg.GetPartitionAssignmentStrategy()
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		log.Info().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " assigned partitions")

		switch partitionAssignmentStrategy {
		case kafkalks.PartitionAssignmentStrategyCooperativeSticky:
			err = tp.consumer.IncrementalAssign(e.Partitions)
			if err != nil {
				logKafkaError(err).Msg(semLogContext + " failed to incrementally assign partitions")
			}
		default:
			err = tp.consumer.Assign(e.Partitions)
			if err != nil {
				logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " assigned partitions")
			}
		}

		tp.partitionsCnt = len(e.Partitions)
		tp.eofCnt = 0
		tp.metricLabels["event-type"] = "assigned-partitions"
		_ = tp.produceMetric(nil, MetricsPartitionsEvents, 1, tp.metricLabels)
		_ = tp.produceMetric(nil, MetricsNumberOfPartitions, float64(len(e.Partitions)), tp.metricLabels)

	case kafka.RevokedPartitions:
		log.Info().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " revoked partitions")
		tp.partitionsCnt = len(e.Partitions)
		tp.eofCnt = 0
		tp.metricLabels["event-type"] = "revoked-partitions"
		_ = tp.produceMetric(nil, MetricsPartitionsEvents, 1, tp.metricLabels)
		_ = tp.produceMetric(nil, MetricsNumberOfPartitions, float64(len(e.Partitions)), tp.metricLabels)

		/*		if err = tp.consumer.Unassign(); err != nil {
				log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " revoked partitions")
			}*/

		switch partitionAssignmentStrategy {
		case kafkalks.PartitionAssignmentStrategyCooperativeSticky:
			if tp.consumer.AssignmentLost() {
				err = tp.abortTransaction(nil)
				if err != nil {
					logKafkaError(err).Msg(semLogContext)
					return err
				}
			} else {
				err = tp.commitTransaction(nil)
				if err != nil {
					logKafkaError(err).Msg(semLogContext + " failed to commit transaction")
					return err
				}
			}

			err = tp.beginTransaction(true)
			if err != nil {
				logKafkaError(err).Msg(semLogContext + " failed to commit transaction")
				return err
			}

			err = tp.consumer.IncrementalUnassign(e.Partitions)
			if err != nil {
				logKafkaError(err).Msg(semLogContext + " failed to commit transaction")
				return err
			}
		default:
			err = tp.abortTransaction(nil)
			if err != nil {
				logKafkaError(err).Msg(semLogContext)
			}

			err = tp.consumer.Unassign()
			if err != nil {
				logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " un-assigned partitions")
			}
		}
	}

	return nil
}

func (tp *transformerProducerImpl) poll() (bool, error) {
	const semLogContext = "t-prod::poll"
	var err error

	isMessage := false
	ev := tp.consumer.Poll(tp.cfg.FromTopic.MaxPollTimeout)
	switch e := ev.(type) {
	//case kafka.AssignedPartitions:
	//	log.Error().Msg(semLogContext + " - should not be here in assignedPartitions")
	//	log.Warn().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " assigned partitions")
	//	/*
	//		if err = tp.consumer.Assign(e.Partitions); err != nil {
	//			log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " assigned partitions")
	//		}*/
	//	tp.partitionsCnt = len(e.Partitions)
	//	tp.eofCnt = 0
	//case kafka.RevokedPartitions:
	//	log.Error().Msg(semLogContext + " - should not be here in revokedPartitions")
	//	/*		log.Warn().Interface("event", e).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " revoked partitions")
	//			if err = tp.consumer.Unassign(); err != nil {
	//				log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " revoked partitions")
	//			}*/
	//
	//	err = tp.abortTransaction(nil, false)
	//	if err != nil {
	//		logKafkaError(err).Msg(semLogContext)
	//	}
	//
	//	tp.partitionsCnt = 0
	//	tp.eofCnt = 0
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
	log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Bool("enabled", kafkalks.IsTransactionCommit(tp.cfg.CommitMode)).Msg(semLogContext)
	if !kafkalks.IsTransactionCommit(tp.cfg.CommitMode) {
		return nil
	}

	kp := tp.getProducer()
	if err := kp.BeginTransaction(); err != nil {
		log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " begin transaction error")
		return err
	}

	return nil
}

func (tp *transformerProducerImpl) abortTransaction(ctx context.Context) error {
	const semLogContext = "t-prod::abort-transaction"
	log.Info().Str(semLogTransformerProducerId, tp.cfg.Name).Bool("tx-enabled", kafkalks.IsTransactionCommit(tp.cfg.CommitMode)).Msg(semLogContext + " aborting transaction...")

	switch tp.cfg.CommitMode {
	case kafkalks.CommitModeAuto:
		return nil
	case kafkalks.CommitModeManual:
		log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " no  action on aborting on manual commit...")

	case kafkalks.CommitModeTransaction:
		kp := tp.getProducer()
		if kp.IsInTransaction() {
			if err := kp.AbortTransaction(ctx); err != nil {
				log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " abort transaction error")
				return err
			} else {
				log.Info().Msg(semLogContext + " transaction succesfully aborted")
			}

			_ = rewindConsumer(tp.consumer)

			err := kp.BeginTransaction()
			if err != nil {
				logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " producer begin transaction")
				return err
			}
		}
		// tp.txActive = false
	default:
		log.Warn().Str("commit-mode", tp.cfg.CommitMode).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " commit-mode not recognized")
	}

	return nil
}

func (tp *transformerProducerImpl) commitTransaction(ctx context.Context) error {
	const semLogContext = "t-prod::commit-transaction"
	log.Trace().Str(semLogTransformerProducerId, tp.cfg.Name).Bool("enabled", kafkalks.IsTransactionCommit(tp.cfg.CommitMode)).Msg(semLogContext)

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
		kp := tp.getProducer()
		if kp.IsInTransaction() {

			partitions, err := tp.consumer.Assignment()
			if err != nil {
				logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer assignment error")
				return err
			}

			positions, err := tp.consumer.Position(partitions)
			if err != nil {
				logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer position error")
				return err
			}

			consumerMetadata, err := tp.consumer.GetConsumerGroupMetadata()
			if err != nil {
				logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer get group metadata")
				return err
			}

			// TODO: replaced ctx with nil.
			err = kp.SendOffsetsToTransaction(nil, positions, consumerMetadata)
			if err != nil {
				logKafkaError(err).Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer send offset to transaction")
				err = kp.AbortTransaction(nil)
				if err != nil {
					return err
				}

				_ = rewindConsumer(tp.consumer)
				return err
			}

			// TODO: replaced ctx with nil.
			err = kp.CommitTransaction(nil)
			// tp.txActive = false
			if err != nil {
				logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " consumer commit transaction")

				err = kp.AbortTransaction(nil)
				if err != nil {
					return err
				}

				_ = rewindConsumer(tp.consumer)
				return err
			}

			err = kp.BeginTransaction()
			if err != nil {
				logKafkaError(err).Str(semLogTransformerProducerId, tp.cfg.Name).Msg(semLogContext + " producer begin transaction")
				return err
			}
		}
	default:
		log.Warn().Str(semLogTransformerProducerId, tp.cfg.Name).Str("commit-mode", tp.cfg.CommitMode).Msg(semLogContext + " commit-mode not recognized")
	}

	return nil
}

func (tp *transformerProducerImpl) getProducerForTopic(topicCfg *ConfigTopic) (*KafkaProducerWrapper, error) {
	n := util.StringCoalesce(topicCfg.BrokerName, tp.cfg.BrokerName)
	if p, ok := tp.producers[n]; ok {
		return p, nil
	}

	return nil, fmt.Errorf("cannot find producer for topic %s in broker %s", topicCfg.Name, n)
}

func (tp *transformerProducerImpl) getProducer() *KafkaProducerWrapper {

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

				if st, err := producer.Produce(km); err != nil {
					log.Error().Err(err).Str(semLogTransformerProducerId, tp.cfg.Name).Int("status", st).Msg(semLogContext + " errors in producing message")
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
