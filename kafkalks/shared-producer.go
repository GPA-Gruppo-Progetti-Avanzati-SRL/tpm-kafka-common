package kafkalks

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkautil"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

func (lks *LinkedService) NewSharedProducer(ctx context.Context, synchDelivery bool) (SharedProducer, error) {
	const semLogContext = "kafka-lks::new-sha-producer"

	var err error
	lks.mu.Lock()
	defer lks.mu.Unlock()

	if lks.sharedProducer.producer == nil {
		lks.sharedProducer.producer, err = lks.NewProducer(ctx, "")
		go lks.monitorSharedProducerAsyncEvents(lks.sharedProducer.producer)
	}

	return lks.sharedProducer, err
}

type ProducerResponse struct {
	BrokerName string       `mapstructure:"broker-name,omitempty" json:"broker-name,omitempty" yaml:"broker-name,omitempty"`
	Topic      string       `mapstructure:"topic,omitempty" json:"topic,omitempty" yaml:"topic,omitempty"`
	Error      string       `mapstructure:"error,omitempty" json:"error,omitempty" yaml:"error,omitempty"`
	Offset     kafka.Offset `mapstructure:"offset,omitempty" json:"offset,omitempty" yaml:"offset,omitempty"`
	Partition  int32        `mapstructure:"partition,omitempty" json:"partition,omitempty" yaml:"partition,omitempty"`
}

const (
	MetricIdStatusCode = "status-code"
	MetricIdBrokerName = "broker-name"
	MetricIdTopicName  = "topic-name"
	MetricIdErrorCode  = "error-code"
)

func (lks *LinkedService) monitorSharedProducerAsyncEvents(producer *kafka.Producer) {
	const semLogContext = "kafka-lks::monitor-sha-producer"
	log.Info().Msg(semLogContext + " starting monitor producer events")

	// Not used at the moment
	// exitFromLoop := false

	metricLabels := prometheus.Labels{
		MetricIdStatusCode: "500",
		MetricIdBrokerName: lks.cfg.BrokerName,
	}

	for e := range producer.Events() {

		delete(metricLabels, MetricIdTopicName)
		delete(metricLabels, MetricIdErrorCode)
		metricLabels[MetricIdStatusCode] = "500"

		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Topic != nil {
				metricLabels[MetricIdTopicName] = *ev.TopicPartition.Topic
			}

			if ev.TopicPartition.Error != nil {
				log.Error().Err(ev.TopicPartition.Error).Interface("event", ev).Msg(semLogContext + " delivery failed")
			} else {
				metricLabels[MetricIdStatusCode] = "200"
				log.Trace().Interface("partition", ev.TopicPartition).Msg(semLogContext + " delivered message")
			}
			_ = setMetrics(lks.cfg.Producer.AsyncDeliveryMetrics, metricLabels)
		case kafka.Error:
			metricLabels[MetricIdErrorCode] = ev.Code().String()
			kafkautil.LogKafkaError(ev).Msg(semLogContext)

			_ = setMetrics(lks.cfg.Producer.AsyncDeliveryMetrics, metricLabels)
		default:
			log.Warn().Str("event-type", fmt.Sprint("%T", ev)).Msg(semLogContext + " un-detected event type")
			log.Warn().Interface("event", ev).Msg(semLogContext + " un-detected event value")
		}

		/*
			if exitFromLoop {
				break
			}
		*/
	}

	log.Info().Msg(semLogContext + " exiting from monitor producer events")
}

func setMetrics(metrics promutil.MetricsConfigReference, lbls prometheus.Labels) error {
	const semLogContext = "kafka-lks::set-monitor-producer-metrics"

	var err error
	var g promutil.Group

	if metrics.IsEnabled() {
		g, _, err = metrics.ResolveGroup(nil)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}

		if metrics.IsCounterEnabled() {
			err = g.SetMetricValueById(metrics.CounterId, 1, lbls)
		}
	}

	return err
}

func (shaProd *SharedProducer) Produce2Topic(topicName string, k, msg []byte, hdrs map[string]string, span opentracing.Span) (int, ProducerResponse) {
	const semLogContext = "kafka-lks::produce-2-topic"
	log.Trace().Str("broker", shaProd.brokerName).Str("topic", topicName).Msg(semLogContext)

	var err error
	if shaProd.producer == nil {
		err = errors.New("producer is nil")
		return http.StatusInternalServerError, ProducerResponse{BrokerName: shaProd.brokerName, Error: err.Error(), Topic: topicName}
	}

	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Key:            k,
		Value:          msg,
	}

	var headers map[string]string
	if span != nil || len(hdrs) > 0 {
		headers = make(map[string]string)

		if span != nil {
			opentracing.GlobalTracer().Inject(
				span.Context(),
				opentracing.TextMap,
				opentracing.TextMapCarrier(headers))
		}

		for headerKey, headerValue := range hdrs {
			headers[headerKey] = headerValue
		}

		for headerKey, headerValue := range headers {
			km.Headers = append(km.Headers, kafka.Header{
				Key:   headerKey,
				Value: []byte(headerValue),
			})
		}
	}

	if err := shaProd.producer.Produce(km, nil); err != nil {
		log.Error().Err(err).Msg(semLogContext + " errors in producing message")
		return http.StatusServiceUnavailable, ProducerResponse{BrokerName: shaProd.brokerName, Error: err.Error(), Topic: topicName}
	}

	return http.StatusAccepted, ProducerResponse{BrokerName: shaProd.brokerName, Topic: topicName}
}
