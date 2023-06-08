package tprod

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

type MetricData struct {
	MetricId string  `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	Value    float64 `yaml:"value,omitempty" mapstructure:"value,omitempty" json:"value,omitempty"`
}

type BAMData struct {
	MetricsData []MetricData      `yaml:"metrics-data,omitempty" mapstructure:"metrics-data,omitempty" json:"metrics-data,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty" mapstructure:"labels,omitempty" json:"labels,omitempty"`
}

func (bd *BAMData) AddBAMData(bamData BAMData) BAMData {

	for n, v := range bamData.Labels {
		bd.AddLabel(n, v)
	}

	for _, md := range bamData.MetricsData {
		bd.MetricsData = append(bd.MetricsData, md)
	}

	return *bd
}

func (bd *BAMData) Set(mid string, val float64) {
	bd.MetricsData = append(bd.MetricsData, MetricData{MetricId: mid, Value: val})
}

func (bd *BAMData) AddLabel(lbl, val string) {
	if bd.Labels == nil {
		bd.Labels = make(map[string]string)
	}

	bd.Labels[lbl] = val
}

func (bd *BAMData) AddMessageHeaders(hs []kafka.Header) {
	for _, h := range hs {
		bd.AddLabel(h.Key, string(h.Value))
	}
}

func (bd *BAMData) GetMetricData(mid string) (MetricData, bool) {

	for _, md := range bd.MetricsData {
		if md.MetricId == mid {
			return md, true
		}
	}

	return MetricData{}, false
}

func (bd *BAMData) GetLabel(lbl string, defValue string) (string, bool) {

	v, ok := bd.Labels[lbl]
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
	for n, l := range bd.Labels {
		log.Trace().Str("label", n).Str("value", l).Msg(semLogContext + ": labels")
	}
	for _, l := range bd.MetricsData {
		log.Trace().Str("metric-id", l.MetricId).Float64("value", l.Value).Msg(semLogContext + ": metrics")
	}

}
