package consumer

import (
	"context"
)

type Handler interface {
	Topic() string
	Handle(ctx context.Context, msg Message) error
}

type funcHandler struct {
	topic string
	fn    func(ctx context.Context, msg Message) error
}

func (h funcHandler) Topic() string { return h.topic }

func (h funcHandler) Handle(ctx context.Context, msg Message) error {
	return h.fn(ctx, msg)
}

func NewHandler(topic string, fn func(ctx context.Context, msg Message) error) Handler {
	return funcHandler{topic: topic, fn: fn}
}

func JSONHandler[T any](topic string, fn func(ctx context.Context, data T) error) Handler {
	return NewHandler(topic, func(ctx context.Context, msg Message) error {
		var data T
		if err := msg.Decode(&data); err != nil {
			return err
		}
		return fn(ctx, data)
	})
}
