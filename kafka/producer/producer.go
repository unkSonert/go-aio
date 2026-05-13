package producer

import (
	"context"
	"encoding/json"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/unkSonert/go-aio/kafka/config"
)

type Producer struct {
	writer *kafka.Writer
}

type publishOptions struct {
	key     []byte
	headers []kafka.Header
	time    time.Time
}

type Option func(*publishOptions)

func WithKey(key []byte) Option {
	return func(options *publishOptions) {
		options.key = key
	}
}

func WithHeaders(headers ...kafka.Header) Option {
	return func(options *publishOptions) {
		options.headers = headers
	}
}

func WithTime(t time.Time) Option {
	return func(options *publishOptions) {
		options.time = t
	}
}

func New(cfg config.Producer) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	writer := kafka.NewWriter(cfg.WriterConfig())
	return &Producer{writer: writer}, nil
}

func (p *Producer) Publish(ctx context.Context, topic string, data any, opts ...Option) error {
	if topic == "" {
		return ErrTopicRequired
	}
	if p == nil || p.writer == nil {
		return ErrClosed
	}
	value, err := json.Marshal(data)
	if err != nil {
		return err
	}
	options := collectOptions(opts...)
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   topic,
		Key:     options.key,
		Value:   value,
		Headers: options.headers,
		Time:    options.time,
	})
}

func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func collectOptions(opts ...Option) publishOptions {
	var options publishOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return options
}
