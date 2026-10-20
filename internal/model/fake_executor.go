package model

import "context"

type FakeExecutor struct {
	DoFunc     func(context.Context, Target, ExecuteRequest) ([]byte, *CallError)
	StreamFunc func(context.Context, Target, ExecuteRequest) (<-chan StreamEvent, *CallError)
}

func (f *FakeExecutor) Do(ctx context.Context, target Target, request ExecuteRequest) ([]byte, *CallError) {
	if f.DoFunc != nil {
		return f.DoFunc(ctx, target, request)
	}
	return nil, nil
}

func (f *FakeExecutor) Stream(ctx context.Context, target Target, request ExecuteRequest) (<-chan StreamEvent, *CallError) {
	if f.StreamFunc != nil {
		return f.StreamFunc(ctx, target, request)
	}
	return nil, nil
}

func (*FakeExecutor) Close() error { return nil }
