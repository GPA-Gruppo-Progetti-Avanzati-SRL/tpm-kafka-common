package kafkahartracer

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/rs/zerolog/log"
	"time"
)

type spanImpl struct {
	hartracing.SimpleSpan
}

func (hs *spanImpl) Finish() error {
	const semLogContext = semLogContextBase + "::finish-span"

	hs.Duration = time.Since(hs.StartTime)
	if len(hs.Entries) > 0 {
		log.Trace().Str("span-id", hs.Id()).Msg(semLogContext + " reporting span")
		_ = hs.Tracer.(*tracerImpl).Report(hs)
	} else {
		log.Warn().Str("span-id", hs.Id()).Msg(semLogContext + " no Entries in span....")
	}

	return nil
}

/*
type spanContextImpl struct {
	logId    string
	parentId string
	traceId  string
}

func (spanCtx spanContextImpl) Id() string {
	return spanCtx.Encode()
}

func (spanCtx spanContextImpl) Encode() string {
	s := fmt.Sprintf("%s:%s:%s", spanCtx.logId, spanCtx.parentId, spanCtx.traceId)
	return s
}

func (spanCtx spanContextImpl) IsZero() bool {
	return spanCtx.logId == "" && spanCtx.parentId == "" && spanCtx.traceId == ""
}

func Decode(ser string) (spanContextImpl, error) {
	sarr := strings.Split(ser, ":")
	if len(sarr) != 3 {
		return spanContextImpl{}, fmt.Errorf("invalid span %s", ser)
	}

	sctx := spanContextImpl{
		logId:    sarr[0],
		parentId: sarr[1],
		traceId:  sarr[2],
	}

	return sctx, nil
}

type spanImpl struct {
	tracer      *tracerImpl
	spanContext spanContextImpl
	creator     har.Creator
	browser     har.Creator
	comment     string
	startTime   time.Time
	duration    time.Duration
	finished    bool
	entries     []*har.Entry
}

func (hs *spanImpl) Finish() error {
	hs.duration = time.Since(hs.startTime)
	if len(hs.entries) > 0 {
		_ = hs.tracer.Report(hs)
	} else {
		log.Warn().Str("span-id", hs.Id()).Msg("no entries in span....")
	}

	return nil
}

func (hs *spanImpl) Id() string {
	return hs.spanContext.Encode()
}

func (hs *spanImpl) Context() hartracing.SpanContext {
	return hs.spanContext
}

func (hs *spanImpl) String() string {
	id := hs.spanContext.Encode()
	return fmt.Sprintf("[%s] #entries: %d - start: %s - dur: %d", id, len(hs.entries), hs.startTime.Format(time.RFC3339Nano), hs.duration.Milliseconds())
}

func (hs *spanImpl) AddEntry(e *har.Entry) error {
	e.TraceId = hs.Id()
	hs.entries = append(hs.entries, e)
	return nil
}

func (hs *spanImpl) GetHARData() (*har.HAR, error) {

	const semLogContext = "log-tracer-span::get-har-data"
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		log.Warn().Msg(semLogContext + " HOSTNAME env variable not set")
		podName = "localhost"
	}

	har := har.HAR{
		Log: &har.Log{
			Version: "1.1",
			Creator: &har.Creator{
				Name:    "tpm-har",
				Version: "1.0",
			},
			Browser: &hs.browser,
			Comment: hs.comment,
			TraceId: hs.Id(),
		},
	}

	for _, e := range hs.entries {
		har.Log.Entries = append(har.Log.Entries, e)
	}

	return &har, nil
}
*/
