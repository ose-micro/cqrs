package cqrs

type Command interface {
	CommandName() string
	Validate() error
}

type Query interface {
	QueryName() string
}
