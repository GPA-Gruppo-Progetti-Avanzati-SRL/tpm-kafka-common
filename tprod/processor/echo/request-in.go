package echo

import (
	"bytes"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"strconv"
	"strings"
)

type RequestIn struct {
	Span        opentracing.Span  `yaml:"-" mapstructure:"-" json:"-"`
	ContentType string            `yaml:"content-type" mapstructure:"content-type" json:"content-type"`
	MessageName string            `yaml:"message-name" mapstructure:"message-name" json:"message-name"`
	Headers     map[string]string `yaml:"headers" mapstructure:"headers" json:"headers"`
	Key         []byte            `yaml:"key" mapstructure:"key" json:"key"`
	Body        []byte            `yaml:"body" mapstructure:"body" json:"body"`
}

const (
	CENumberOfAttempts = "ce_no_attempts" //  Number of times the message has been processed
	CEIsError          = "ce_is_error"
)

func (r *RequestIn) GetHeaderAsInt(hn string) int {
	const semLogContext = "echo::get-header-as-int"
	h := r.Header(hn)
	if h == "" {
		log.Info().Str("name", hn).Msg(semLogContext + " not found")
		return 1
	}

	ih, err := strconv.Atoi(h)
	if err != nil {
		log.Error().Err(err).Str(hn, h).Msg("message header " + hn + " invalid format")
		return 1
	}

	return ih
}

func (r *RequestIn) GetHeaderAsBool(hn string) bool {

	h := r.Header(hn)
	if h == "" {
		return false
	}

	ih, err := strconv.ParseBool(h)
	if err != nil {
		log.Error().Err(err).Str(hn, h).Msg("message header " + hn + " invalid format")
		return false
	}

	return ih
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

	headers := make(map[string]string)
	for _, header := range km.Headers {
		headers[header.Key] = string(header.Value)
	}
	req.Headers = headers

	var ct string
	var ok bool
	if ct, ok = req.Headers[KMContentType]; !ok {
		ct = "application/octet-stream"
	} else {
		// remove the semicolon (if present to clean up the content type) to text/xml; charset=utf-8
		ndx := strings.Index(ct, ";")
		if ndx > 0 {
			ct = ct[ndx:]
		}
	}

	if ct == "application/xml" {
		ct = "text/xml"
	}

	req.ContentType = ct

	// Echo mode.... tolerant. Try to intercept the document-type
	if ct == "text/xml" {
		ndxStart := bytes.Index(km.Value, []byte("xmlns=\"urn:iso:std:iso:20022:tech:xsd:"))
		if ndxStart >= 0 {
			ndxStart += len("xmlns=\"urn:iso:std:iso:20022:tech:xsd:")
			ndxEnd := bytes.Index(km.Value[ndxStart:], []byte("\""))
			if ndxEnd > 0 {
				req.MessageName = string(km.Value[ndxStart : ndxStart+ndxEnd])
			}
		}
	}

	req.Key = km.Key
	req.Body = km.Value

	//spanContext, _ := opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapCarrier(headers))
	//log.Trace().Bool("span-from-message", spanContext != nil).Msg(semLogContext)
	//if spanContext != nil {
	//	req.Span = opentracing.StartSpan(spanName, opentracing.FollowsFrom(spanContext))
	//} else {
	//	req.Span = opentracing.StartSpan(spanName)
	//}

	req.Span = span

	if req.GetHeaderAsBool(CEIsError) {
		err = errors.New("error triggered by header")
	}

	return req, err
}

func (r *RequestIn) Finish() {
	if r.Span != nil {
		r.Span.Finish()
	}
}
