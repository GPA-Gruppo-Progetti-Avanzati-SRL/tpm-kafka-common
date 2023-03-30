package kafkahartracer_test

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/har"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-kafka-har/kafkahartracer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestKafkaTracer(t *testing.T) {
	cfg := kafkalks.Config{
		BrokerName:       "local",
		BootstrapServers: "localhost:9092",
		SecurityProtocol: "PLAIN",
		SSL: kafkalks.SSLCfg{
			CaLocation: "",
			SkipVerify: true,
		},
		Producer: kafkalks.ProducerConfig{
			Acks:         "all",
			MaxTimeoutMs: 100000,
		},
	}

	trc, c, err := kafkahartracer.NewTracer(kafkahartracer.WithKafkaConfig(&cfg), kafkahartracer.WithTopic("har-tracing"))
	require.NoError(t, err)
	if c != nil {
		defer c.Close()
	}

	s := trc.StartSpan()
	s.AddEntry(&har.Entry{
		StartedDateTime: "2023-02-12T20:07:02.147874+01:00",
		Time:            5,
		Request: &har.Request{
			Method:      "POST",
			URL:         "/examples/example-001/api/v1/orc-001",
			HTTPVersion: "1.1",
			Cookies:     []har.Cookie{},
			Headers: []har.NameValuePair{
				{
					Name:  "Requestid",
					Value: "a-resquest-id",
				},
				{
					Name:  "Content-Type",
					Value: "application/json",
				},
			},
			QueryString: har.NameValuePairs{},
			PostData: &har.PostData{
				MimeType: "application/json",
				Params:   har.Params{},
				Text:     "\"{\\n  \\\"canale\\\": \\\"APPP\\\",\\n  \\\"ordinante\\\": {\\n    \\\"natura\\\": \\\"PP\\\",\\n    \\\"tipologia\\\": \\\"ALIAS\\\",\\n    \\\"numero\\\": \\\"10724279\\\",\\n    \\\"codiceFiscale\\\": \\\"77626979028\\\",\\n    \\\"intestazione\\\": \\\"string\\\"\\n  }\\n}\"",
				Comment:  "",
				Data:     nil,
			},
			HeadersSize: -1,
			BodySize:    101,
			Comment:     "",
		},
		Response: &har.Response{
			Status:      503,
			StatusText:  "execution error",
			HTTPVersion: "1.1",
			Cookies:     []har.Cookie{},
			Headers: []har.NameValuePair{
				{
					Name:  "Content-Type",
					Value: "application/json",
				},
			},
			Content: &har.Content{
				Size:        82,
				Compression: 0,
				MimeType:    "application/json",
				Text:        "{\\\"ambit\\\":\\\"endpoint01\\\",\\\"step\\\":\\\"endpoint01\\\",\\\"timestamp\\\":\\\"2023-02-12T20:07:02+01:00\\\"}",
				Encoding:    "",
				Comment:     "",
				Data:        nil,
			},
			RedirectURL: "",
			HeadersSize: -1,
			BodySize:    82,
			Comment:     "",
		},
		Cache: nil,
		Timings: &har.Timings{
			Blocked: -1,
			DNS:     -1,
			Connect: -1,
			Send:    -1,
			Wait:    5,
			Receive: -1,
			Ssl:     -1,
			Comment: "",
		},
		ServerIPAddress: "",
		Connection:      "",
		Comment:         "",
		TraceId:         "",
	})

	s.Finish()

}
