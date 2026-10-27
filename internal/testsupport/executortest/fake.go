package executortest

import (
	"context"
	"github.com/mytecor/llm-gateway/internal/model"
)

type FakeExecutor struct {
	DoFunc     func(context.Context, model.Target, model.ExecuteRequest) ([]byte, *model.CallError)
	StreamFunc func(context.Context, model.Target, model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError)
}

func (f *FakeExecutor) Do(ctx context.Context, target model.Target, request model.ExecuteRequest) ([]byte, *model.CallError) {
	if f.DoFunc != nil {
		return f.DoFunc(ctx, target, request)
	}
	return nil, nil
}

func (f *FakeExecutor) Stream(ctx context.Context, target model.Target, request model.ExecuteRequest) (<-chan model.StreamEvent, *model.CallError) {
	if f.StreamFunc != nil {
		return f.StreamFunc(ctx, target, request)
	}
	return nil, nil
}

func (*FakeExecutor) Close() error { return nil }
