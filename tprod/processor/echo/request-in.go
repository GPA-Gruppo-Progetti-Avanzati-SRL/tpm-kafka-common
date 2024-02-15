package echo

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod"
	"github.com/rs/zerolog/log"
	"strconv"
	"strings"
)

type RequestIn struct {
	// Span            opentracing.Span      `yaml:"-" mapstructure:"-" json:"-"`
	ContentType string `yaml:"content-type" mapstructure:"content-type" json:"content-type"`
	msg         tprod.Message
	/*
		MessageName     string                `yaml:"message-name" mapstructure:"message-name" json:"message-name"`
		Headers         map[string]string     `yaml:"headers" mapstructure:"headers" json:"headers"`
		Key             []byte                `yaml:"key" mapstructure:"key" json:"key"`
		Body            []byte                `yaml:"body" mapstructure:"body" json:"body"`
		MessageProducer tprod.MessageProducer `yaml:"message-producer" mapstructure:"message-producer" json:"message-producer"`
	*/

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
	if len(r.msg.Headers) > 0 {
		return r.msg.Headers[hn]
	}
	return ""
}

func newRequestIn(m tprod.Message) (RequestIn, error) {

	const semLogContext = "echo-blob::new-request-in"

	var req RequestIn
	var err error

	/*
		headers := make(map[string]string)
		for _, header := range km.Headers {
			headers[header.Key] = string(header.Value)
		}
		req.Headers = headers
	*/

	var ct string
	var ok bool
	if ct, ok = req.msg.Headers[KMContentType]; !ok {
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
	req.msg = m

	if req.GetHeaderAsBool(CEIsError) {
		err = errors.New("error triggered by header")
	}

	return req, err
}

/*
func (r *RequestIn) Finish() {
	if r.Span != nil {
		r.Span.Finish()
	}
}
*/
