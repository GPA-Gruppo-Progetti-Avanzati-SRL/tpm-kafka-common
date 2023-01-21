package tprod

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

type MetricData struct {
	metricId string  `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	value    float64 `yaml:"value,omitempty" mapstructure:"value,omitempty" json:"value,omitempty"`
}

type BAMData struct {
	MetricsData []MetricData      `yaml:"metrics-data,omitempty" mapstructure:"metrics-data,omitempty" json:"metrics-data,omitempty"`
	labels      map[string]string `yaml:"labels,omitempty" mapstructure:"labels,omitempty" json:"labels,omitempty"`
}

func (bd *BAMData) AddBAMData(bamData BAMData) BAMData {

	for n, v := range bamData.labels {
		bd.AddLabel(n, v)
	}

	for _, md := range bamData.MetricsData {
		bd.MetricsData = append(bd.MetricsData, md)
	}

	return *bd
}

func (bd *BAMData) Set(mid string, val float64) {
	bd.MetricsData = append(bd.MetricsData, MetricData{metricId: mid, value: val})
}

func (bd *BAMData) AddLabel(lbl, val string) {
	if bd.labels == nil {
		bd.labels = make(map[string]string)
	}

	bd.labels[lbl] = val
}

func (bd *BAMData) AddMessageHeaders(hs []kafka.Header) {
	for _, h := range hs {
		bd.AddLabel(h.String(), string(h.Value))
	}
}

func (bd *BAMData) GetMetricData(mid string) (MetricData, bool) {

	for _, md := range bd.MetricsData {
		if md.metricId == mid {
			return md, true
		}
	}

	return MetricData{}, false
}

func (bd *BAMData) GetLabel(lbl string, defValue string) (string, bool) {

	v, ok := bd.labels[lbl]
	if v == "" {
		v = defValue
	}
	return v, ok
}

func (bd *BAMData) String() string {
	b, err := json.Marshal(bd)
	if err != nil {
		return err.Error()
	}

	return string(b)
}

func (bd *BAMData) Trace() {

	const semLogContext = "bamdata trace"
	for n, l := range bd.labels {
		log.Trace().Str("label", n).Str("value", l).Msg(semLogContext + ": labels")
	}
	for _, l := range bd.MetricsData {
		log.Trace().Str("metric-id", l.metricId).Float64("value", l.value).Msg(semLogContext + ": metrics")
	}

}
