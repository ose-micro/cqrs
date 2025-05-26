package cqrs

type Command interface {
	CommandName() string
	Validate() error
}

type Event interface {
	EventName() string
}

type Query interface {
	QueryName() string
}
