package cqrs

import "context"

type CommandHandle[C Command, R any] interface {
	Handle(ctx context.Context, command C) (R, error)
}

type EventHandle[E Event] interface {
	Handle(ctx context.Context, event E) error
}

type QueryHandle[Q Query, R any] interface {
	Handle(ctx context.Context, query Q) (R, error)
}
