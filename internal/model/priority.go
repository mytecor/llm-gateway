package model

import "context"

type Priority int

const (
	PriorityBackground Priority = iota
	PriorityNormal
	PriorityInteractive
)

func (p Priority) String() string {
	return [...]string{"background", "normal", "interactive"}[p]
}
func ParsePriority(s string) (Priority, bool) {
	switch s {
	case "interactive":
		return PriorityInteractive, true
	case "normal":
		return PriorityNormal, true
	case "background":
		return PriorityBackground, true
	}
	return PriorityNormal, false
}

type priorityKey struct{}

func WithPriority(ctx context.Context, p Priority) context.Context {
	return context.WithValue(ctx, priorityKey{}, p)
}
func RequestPriority(ctx context.Context) Priority {
	if p, ok := ctx.Value(priorityKey{}).(Priority); ok {
		return p
	}
	return PriorityNormal
}
