package echo_test

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod/processor/echo"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

var cfg kafkalks.Config
var echoCfg echo.Config
var meticsCfg map[string]promutil.MetricGroupConfig

func TestMain(m *testing.M) {

	cfg = kafkalks.Config{
		BrokerName:       "local",
		BootstrapServers: "localhost:9092",
		SecurityProtocol: "PLAIN",
		SSL: kafkalks.SSLCfg{
			CaLocation: "",
			SkipVerify: true,
		},
		SASL: kafkalks.SaslCfg{
			Mechanisms: "PLAIN",
			Username:   "$ConnectionString",
			Password:   "Endpoint=sb://testgect.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fIx/54sQHwdKbjmkQyAW5qVkaZf2Tyi7Vk8fPhK8SSI=",
			CaLocation: "",
			SkipVerify: false,
		},
		Consumer: kafkalks.ConsumerConfig{
			IsolationLevel:     "read_committed",
			MaxPollRecords:     500,
			AutoOffsetReset:    "earliest",
			SessionTimeoutMs:   30000,
			FetchMinBytes:      1,
			FetchMaxBytes:      3000000,
			Delay:              2000,
			MaxRetry:           1,
			EnablePartitionEOF: true,
		},
		Producer: kafkalks.ProducerConfig{
			Acks:         "all",
			MaxTimeoutMs: 100000,
		},
	}

	meticsCfg = map[string]promutil.MetricGroupConfig{
		"tp-echo": promutil.MetricGroupConfig{
			Namespace: "kafkalks",
			Subsystem: "echotest",
			Collectors: []promutil.MetricConfig{
				{
					Id:   "tprod-errors",
					Name: "errors",
					Help: "numero errori",
					Labels: []promutil.MetricLabelConfig{
						{
							Name:         "ce_dataset",
							DefaultValue: "DS-NA",
						},
					},
					Type:    "counter",
					Buckets: promutil.HistogramBucketConfig{},
				},
				{
					Id:   "tprod-events",
					Name: "messages",
					Help: "numero messaggi",
					Labels: []promutil.MetricLabelConfig{
						{
							Name:         "ce_dataset",
							DefaultValue: "DS-NA",
						},
					},
					Type:    "counter",
					Buckets: promutil.HistogramBucketConfig{},
				},
				{
					Id:   "tprod-events-to-topic",
					Name: "messages_to_topic",
					Help: "numero messaggi prodotti su topic",
					Labels: []promutil.MetricLabelConfig{
						{
							Name:         "topic",
							DefaultValue: "NA",
						},
						{
							Name:         "topic_type",
							DefaultValue: "NA",
						},
					},
					Type:    "counter",
					Buckets: promutil.HistogramBucketConfig{},
				},
				{
					Id:   "tprod-duration",
					Name: "duration",
					Help: "durata lavorazione",
					Labels: []promutil.MetricLabelConfig{
						{
							Name:         "ce_dataset",
							DefaultValue: "DS-NA",
						},
					},
					Type: "histogram",
					Buckets: promutil.HistogramBucketConfig{
						Type:        "linear",
						Start:       0.5,
						WidthFactor: 0.5,
						Count:       10,
					},
				},
			},
		},
	}

	_, err := promutil.InitRegistry(meticsCfg)
	if nil != err {
		log.Fatal().Err(err).Msg("metrics registry initialization error")
	}

	echoCfg = echo.Config{
		TransformerProducerConfig: &tprod.TransformerProducerConfig{
			Name:         "tp-echo",
			WorkMode:     tprod.WorkModeBatch,
			TickInterval: time.Millisecond * 400,
			OnErrors: []tprod.OnErrorPolicy{
				{tprod.OnErrorLevelFatal, tprod.OnErrorDeadLetter},
				{tprod.OnErrorLevelError, tprod.OnErrorDeadLetter},
			},
			OnEof:     "", // tprod.OnEofExit,
			EofAfterN: 0,
			RefMetrics: &promutil.MetricsConfigReference{
				GId: "tp-echo",
			},
			CommitMode: "manual", // auto, manual, tx
			GroupId:    "rtp-bconn-rework-iso20022-dlt-gid9",
			ProducerId: "",
			BrokerName: "local",
			FromTopic: tprod.ConfigTopic{
				Id:             "echo-in",
				Name:           "echo-in",
				BrokerName:     "local",
				MaxPollTimeout: 100,
				TopicType:      "",
			},
			ToTopics: []tprod.ConfigTopic{
				{
					Id:             "echo-out",
					Name:           "echo-out",
					BrokerName:     "local",
					MaxPollTimeout: 0,
					TopicType:      "std",
				},
				{
					Id:             "echo-dlt",
					Name:           "echo-dlt",
					BrokerName:     "local",
					MaxPollTimeout: 0,
					TopicType:      "dead-letter",
				},
			},
		},
		ProcessorConfig: &echo.ProcessorConfig{
			NumRetries:                 1,
			NumberOfAttemptsHeaderName: "",
		},
	}
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestEcho(t *testing.T) {

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	c, err := InitTracing(t)
	require.NoError(t, err)
	defer c.Close()

	_, err = kafkalks.Initialize([]kafkalks.Config{cfg})
	require.NoError(t, err)

	var wg sync.WaitGroup
	tp, err := echo.NewEcho(&echoCfg, &wg)
	require.NoError(t, err)

	tp.Start()

	wg.Wait()
	t.Log("exiting...")
}

const (
	JAEGER_SERVICE_NAME = "JAEGER_SERVICE_NAME"
)

func InitTracing(t *testing.T) (io.Closer, error) {

	if os.Getenv(JAEGER_SERVICE_NAME) == "" {
		t.Log("skipping jaeger config no vars in env.... (" + JAEGER_SERVICE_NAME + ")")
		return nil, nil
	}

	t.Log("initialize jaeger service " + os.Getenv(JAEGER_SERVICE_NAME))

	var tracer opentracing.Tracer
	var closer io.Closer

	jcfg, err := jaegercfg.FromEnv()
	if err != nil {
		log.Warn().Err(err).Msg("Unable to configure JAEGER from environment")
		return nil, err
	}

	tracer, closer, err = jcfg.NewTracer(
		jaegercfg.Logger(&jlogger{}),
		jaegercfg.Metrics(metrics.NullFactory),
	)
	if nil != err {
		log.Error().Err(err).Msg("Error in NewTracer")
		return nil, err
	}

	opentracing.SetGlobalTracer(tracer)

	return closer, nil
}

type jlogger struct{}

func (l *jlogger) Error(msg string) {
	log.Error().Msg("(jaeger) " + msg)
}

func (l *jlogger) Infof(msg string, args ...interface{}) {
	log.Info().Msgf("(jaeger) "+msg, args...)
}
