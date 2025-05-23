package cqrs

import "context"

type CommandHandle[T Command] interface {
	Handle(ctx context.Context, cmd T) error
}

type QueryHandle[Q Query, R any] interface {
	Handle(ctx context.Context, query Q) (R, error)
}
