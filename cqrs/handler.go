package cqrs

import "context"

type CommandHandle[cmd Command] interface {
	Handle(ctx context.Context, cmd Command) error
}

type QueryHandle[query Query, R any] interface {
	Handle(ctx context.Context, query Query) (R, error)
}
