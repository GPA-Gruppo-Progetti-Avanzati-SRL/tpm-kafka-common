package tprod

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
)

const (
	MessageProducerBufferSize = 1000
)

type MessageProducer interface {
	Produce(...Message) error
	Flush() error
	Release()
	Close() error
	NumberOfProducedMessages() int
}

type messageProducerImpl struct {
	buffered         bool
	producedMessages int
	DeadLetterTopic  string
	OutTopicsCfg     []ConfigTopic
	producer         *kafka.Producer
	messages         []Message
	metricsGroupId   string
	metricsLabels    map[string]string
}

func NewMessageProducer(name string, producer *kafka.Producer, buffered bool, outs []ConfigTopic, metricsGroupId string) MessageProducer {
	return &messageProducerImpl{
		buffered:       buffered,
		producer:       producer,
		OutTopicsCfg:   outs,
		metricsGroupId: metricsGroupId,
		metricsLabels: map[string]string{
			"name": name,
		},
	}
}

func (p *messageProducerImpl) Produce(msgs ...Message) error {

	var err error

	if p.buffered {
		p.messages = append(p.messages, msgs...)
	} else {

		if MessageProducerBufferSize < 2 {
			p.produce2Topics(msgs)
			p.producedMessages += len(msgs)
		} else {
			p.messages = append(p.messages, msgs...)
			if len(p.messages) >= MessageProducerBufferSize {
				err = p.produce2Topics(p.messages)
				p.producedMessages += len(p.messages)
				p.messages = nil
			}
		}
	}
	return err
}

func (p *messageProducerImpl) Close() error {
	err := p.Flush()
	p.Release()
	return err
}

func (p *messageProducerImpl) Flush() error {

	var err error
	if len(p.messages) > 0 {
		err = p.produce2Topics(p.messages)
		if !p.buffered {
			p.producedMessages += len(p.messages)
		}
	}

	return err
}

func (p *messageProducerImpl) Release() {
	p.producedMessages = 0
	p.messages = nil
}

func (p *messageProducerImpl) NumberOfProducedMessages() int {
	if p.buffered {
		return len(p.messages)
	}
	return p.producedMessages
}

func (p *messageProducerImpl) GetTopicConfig(topic TargetTopic) (ConfigTopic, error) {
	const semLogContext = "message-producer::get-topic-config"
	for _, c := range p.OutTopicsCfg {
		if topic.Id != "" && c.Id == topic.Id {
			return c, nil
		} else {
			if topic.Id == "" && c.TopicType == topic.TopicType {
				return c, nil
			}
		}
	}

	err := errors.New("topic cannot be found in config")
	log.Error().Err(err).Str("id", topic.Id).Str("type", string(topic.TopicType)).Msg(semLogContext)
	return ConfigTopic{}, err
}

func (p *messageProducerImpl) produce2Topics(messages []Message) error {
	const semLogContext = "message-producer::produce-top-topics"
	if p.buffered {
		log.Info().Int("number-of-messages", len(messages)).Msg("produce2Topics")
	}
	for i, m := range messages {

		if i > 0 && i%20000 == 0 {
			fl := p.producer.Flush(10000)
			for fl != 0 {
				log.Info().Int("flushing", fl).Msg(semLogContext)
				fl = p.producer.Flush(10000)
			}
		}

		if err := p.produce2Topic(m); err != nil {
			return err
		}
	}

	p.producer.Flush(15 * 1000)
	return nil
}

func (p *messageProducerImpl) produce2Topic(m Message) error {
	const semLogContext = "message-producer::produce-top-topic"
	if tcfg, err := p.GetTopicConfig(m.ToTopic); err != nil {
		return err
	} else {
		// _ = level.Debug(p.logger).Log(system.DefaultLogMessageField, "producing message", "topic", tcfg.Name)

		km := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &tcfg.Name, Partition: kafka.PartitionAny},
			Key:            m.Key,
			Value:          m.Body,
		}

		if m.Span != nil {

			headers := make(map[string]string)
			opentracing.GlobalTracer().Inject(
				m.Span.Context(),
				opentracing.TextMap,
				opentracing.TextMapCarrier(headers))

			for headerKey, headerValue := range headers {
				km.Headers = append(km.Headers, kafka.Header{
					Key:   headerKey,
					Value: []byte(headerValue),
				})
			}
		}

		if err := p.producer.Produce(km, nil); err != nil {
			log.Error().Err(err).Msg(semLogContext)
			p.metricsLabels["status-code"] = "500"
			p.metricsLabels["topic-name"] = tcfg.Name
			p.produceMetric(nil, MetricMessagesToTopic, 1, p.metricsLabels)
			return err
		}
	}

	return nil
}

func (p *messageProducerImpl) produceMetric(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
	const semLogContext = "t-prod::produce-metric"

	var err error
	if metricGroup == nil {
		g, err := promutil.GetGroup(p.metricsGroupId)
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
