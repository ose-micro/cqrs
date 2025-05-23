# ose-micro/cqrs

> A lightweight, extensible Go package for implementing the CQRS (Command Query Responsibility Segregation) pattern with message bus support.

[![Go Reference](https://pkg.go.dev/badge/github.com/ose-micro/cqrs.svg)](https://pkg.go.dev/github.com/ose-micro/cqrs)
[![Go Report Card](https://goreportcard.com/badge/github.com/ose-micro/cqrs)](https://goreportcard.com/report/github.com/ose-micro/cqrs)
[![License](https://img.shields.io/github/license/ose-micro/cqrs)](LICENSE)

## Overview

`ose-micro/cqrs` provides a simple yet powerful CQRS toolkit for Go microservices. It includes:
- Generic `CommandBus` and `QueryBus` implementations.
- Support for external buses like NATS.
- Middleware support (logging, tracing, metrics, retries).

Built with generics to simplify development while keeping type safety and performance.

## Features

- ✅ Strongly typed command and query handlers using Go generics.
- ✅ Middleware chaining for cross-cutting concerns.
- ✅ Native support for NATS and easy extensibility for Kafka/Redis.
- ✅ Pluggable message bus interface.
- ✅ Clear separation of write and read models.

## Installation

```bash
go get github.com/ose-micro/cqrs
