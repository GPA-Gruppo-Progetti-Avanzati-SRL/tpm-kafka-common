package kafkahartracemerge

import (
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/har"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"strings"
)

type RequestIn struct {
	Span        opentracing.Span  `yaml:"-" mapstructure:"-" json:"-"`
	ContentType string            `yaml:"content-type,omitempty" mapstructure:"content-type,omitempty" json:"content-type,omitempty"`
	TraceId     string            `yaml:"trace-id,omitempty" mapstructure:"trace-id,omitempty" json:"trace-id,omitempty"`
	Headers     map[string]string `yaml:"headers,omitempty" mapstructure:"headers,omitempty" json:"headers,omitempty"`
	Har         *har.HAR          `yaml:"har,omitempty" mapstructure:"har,omitempty" json:"har,omitempty"`
}

func (r *RequestIn) Header(hn string) string {
	if len(r.Headers) > 0 {
		return r.Headers[hn]
	}
	return ""
}

func newRequestIn(km *kafka.Message, span opentracing.Span) (RequestIn, error) {

	const semLogContext = "echo-blob::new-request-in"

	var req RequestIn
	var err error

	req.ContentType = "application/octet-stream"
	headers := make(map[string]string)
	for _, header := range km.Headers {
		headers[header.Key] = string(header.Value)
		switch header.Key {
		case KMContentType:
			req.ContentType = string(header.Value)
			ndx := strings.Index(string(header.Value), ";")
			if ndx > 0 {
				req.ContentType = req.ContentType[ndx:]
			}
		case hartracing.HARTraceIdHeaderName:
			req.TraceId = string(header.Value)
		}
	}
	req.Headers = headers

	var harLog har.HAR
	err = json.Unmarshal(km.Value, &harLog)
	if err != nil {
		return req, err
	}

	req.Har = &harLog
	req.Span = span

	if req.TraceId == "" {
		err = errors.New("har-trac-id missing from message")
	}
	return req, err
}

func (r *RequestIn) Finish() {
	if r.Span != nil {
		r.Span.Finish()
	}
}
