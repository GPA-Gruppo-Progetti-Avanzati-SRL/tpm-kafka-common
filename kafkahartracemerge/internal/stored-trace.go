package internal

import (
	"context"
	"encoding/json"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/har"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
)

type StoredTrace struct {
	Id              string      `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	Pkey            string      `yaml:"pkey,omitempty" mapstructure:"pkey,omitempty" json:"pkey,omitempty"`
	StartedDateTime string      `json:"startedDateTime" yaml:"startedDateTime" mapstructure:"startedDateTime"`
	TTL             int64       `yaml:"ttl,omitempty" mapstructure:"ttl,omitempty" json:"ttl,omitempty"`
	Trace           *har.HAR    `yaml:"trace,omitempty" mapstructure:"trace,omitempty" json:"trace,omitempty"`
	ETag            azcore.ETag `yaml:"-" mapstructure:"-" json:"-"`
}

func (st *StoredTrace) MustToJson() []byte {
	b, err := json.Marshal(st)
	if err != nil {
		panic(err)
	}

	return b
}

func InsertTrace(ctx context.Context, client *azcosmos.ContainerClient, traceId string, traceTtl int64, trace *har.HAR) (StoredTrace, error) {

	sctx, err := hartracing.ExtractSimpleSpanContextFromString(traceId)
	if err != nil {
		return StoredTrace{}, err
	}

	st := StoredTrace{
		Id:              sctx.LogId,
		Pkey:            sctx.LogId,
		StartedDateTime: trace.Log.FindEarliestStartedDateTime(),
		Trace:           trace,
		TTL:             traceTtl,
	}

	resp, err := client.CreateItem(ctx, azcosmos.NewPartitionKeyString(sctx.LogId), st.MustToJson(), nil)
	if err != nil {
		return StoredTrace{}, cosutil.MapAzCoreError(err)
	}

	st.ETag = resp.ETag
	return st, nil
}

func DeleteTrace(ctx context.Context, client *azcosmos.ContainerClient, traceId string) (bool, error) {
	var err error

	sctx, err := hartracing.ExtractSimpleSpanContextFromString(traceId)
	if err != nil {
		return false, err
	}

	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(sctx.LogId), sctx.LogId, nil)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func FindTraceById(ctx context.Context, client *azcosmos.ContainerClient, traceId string) (StoredTrace, error) {

	var result StoredTrace

	sctx, err := hartracing.ExtractSimpleSpanContextFromString(traceId)
	if err != nil {
		return result, err
	}

	resp, err := client.ReadItem(ctx, azcosmos.NewPartitionKeyString(sctx.LogId), sctx.LogId, nil)
	if err != nil {
		return result, cosutil.MapAzCoreError(err)
	}

	err = json.Unmarshal(resp.Value, &result)
	if err != nil {
		return result, err
	}

	result.ETag = resp.ETag
	return result, err
}

func (st *StoredTrace) Replace(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	b, err := json.Marshal(st)
	if err != nil {
		return false, err
	}

	opts := &azcosmos.ItemOptions{IfMatchEtag: &st.ETag}
	resp, err := client.ReplaceItem(ctx, azcosmos.NewPartitionKeyString(st.Pkey), st.Id, b, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	st.ETag = resp.ETag
	return true, nil
}

func (st *StoredTrace) Delete(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {

	var err error

	opts := &azcosmos.ItemOptions{IfMatchEtag: &st.ETag}
	_, err = client.DeleteItem(ctx, azcosmos.NewPartitionKeyString(st.Pkey), st.Id, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	return true, nil
}

func (st *StoredTrace) Upsert(ctx context.Context, client *azcosmos.ContainerClient) (bool, error) {
	b, err := json.Marshal(st)
	if err != nil {
		return false, err
	}

	opts := &azcosmos.ItemOptions{IfMatchEtag: &st.ETag}
	resp, err := client.UpsertItem(ctx, azcosmos.NewPartitionKeyString(st.Pkey), b, opts)
	if err != nil {
		return false, cosutil.MapAzCoreError(err)
	}

	st.ETag = resp.ETag
	return true, nil
}
