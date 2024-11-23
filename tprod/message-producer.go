package tprod

import (
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"sync"
)

const (
	MessageProducerBufferSize = 1
)

type MessageProducer interface {
	Produce(...Message) error
	Flush() error
	Release()
	Close() error
	NumberOfProducedMessages() int
}

type messageProducerImpl struct {
	bufferSize       int
	producedMessages int
	DeadLetterTopic  string
	OutTopicsCfg     []ConfigTopic
	producer         KafkaProducerWrapper
	messages         []Message
	metricsGroupId   string
	metricsLabels    map[string]string
	mu               sync.Mutex
}

func NewMessageProducer(name string, producer KafkaProducerWrapper, bufferSize int, outs []ConfigTopic, metricsGroupId string) MessageProducer {
	return &messageProducerImpl{
		bufferSize:     bufferSize + 1000,
		producer:       producer,
		OutTopicsCfg:   outs,
		metricsGroupId: metricsGroupId,
		metricsLabels: map[string]string{
			"name": name,
		},
	}
}

func (mp *messageProducerImpl) isBuffered() bool {
	return mp.bufferSize > 1
}

func (mp *messageProducerImpl) isOverQuota() bool {
	return len(mp.messages) >= mp.bufferSize
}

func (p *messageProducerImpl) Produce(msgs ...Message) error {
	const semLogContext = "message-producer::produce"
	var err error

	p.mu.Lock()
	defer p.mu.Unlock()

	producedMsgs := 0
	if !p.isBuffered() {
		err = p.produce2Topics(msgs)
		if err != nil {
			logKafkaError(err).Msg(semLogContext)
		} else {
			producedMsgs = len(msgs)
		}
	} else {
		p.messages = append(p.messages, msgs...)
		if p.isOverQuota() {
			err = p.produce2Topics(p.messages)
			if err != nil {
				logKafkaError(err).Msg(semLogContext)
			} else {
				producedMsgs = len(p.messages)
			}
			p.messages = nil
		}
	}

	p.producedMessages += producedMsgs
	if err != nil {
		logKafkaError(err).Msg(semLogContext)
	}

	return err
}

func (p *messageProducerImpl) Close() error {
	err := p.Flush()
	p.Release()
	return err
}

func (p *messageProducerImpl) Flush() error {
	const semLogContext = "message-producer::flush"
	var err error
	if len(p.messages) > 0 {
		err = p.produce2Topics(p.messages)
		if err != nil {
			logKafkaError(err).Msg(semLogContext)
		} else {
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
	const semLogContext = "message-producer::produce-to-topics"
	if p.isBuffered() {
		log.Warn().Int("number-of-messages", len(messages)).Msg(semLogContext)
	}
	for i, m := range messages {

		if i > 0 && i%20000 == 0 {
			fl := p.producer.Flush(10000)
			for fl != 0 {
				log.Warn().Int("flushing", fl).Msg(semLogContext)
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
	const semLogContext = "message-producer::produce-to-topic"
	if tcfg, err := p.GetTopicConfig(m.ToTopic); err != nil {
		return err
	} else {
		// _ = level.Debug(p.logger).Log(system.DefaultLogMessageField, "producing message", "topic", tcfg.Name)

		var hdrs []kafka.Header
		for n, h := range m.Headers {
			hdrs = append(hdrs, kafka.Header{Key: n, Value: []byte(h)})
		}
		km := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &tcfg.Name, Partition: kafka.PartitionAny},
			Key:            m.Key,
			Value:          m.Body,
			Headers:        hdrs,
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

		st, err := p.producer.Produce(km)
		p.metricsLabels["status-code"] = fmt.Sprint(st)
		p.metricsLabels["topic-name"] = tcfg.Name
		p.produceMetric(nil, MetricMessagesToTopic, 1, p.metricsLabels)
		if err != nil {
			logKafkaError(err).Msg(semLogContext)
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
