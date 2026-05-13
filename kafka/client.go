package kafkax

import (
	"context"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/unkSonert/go-aio/kafka/config"
	"github.com/unkSonert/go-aio/kafka/consumer"
	"github.com/unkSonert/go-aio/kafka/producer"
)

type Config = config.Config
type TLS = config.TLS
type SASL = config.SASL
type DLQ = config.DLQ

type Header = kafka.Header

type Message = consumer.Message
type Handler = consumer.Handler
type Producer = producer.Producer
type Consumer = consumer.Consumer
type ProducerOption = producer.Option

type Client struct {
	config Config
}

func New(cfg Config) (*Client, error) {
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Client{config: cfg}, nil
}

func (c *Client) Config() Config {
	if c == nil {
		return Config{}
	}
	return c.config
}

func (c *Client) Producer() (*Producer, error) {
	cfg, err := c.config.ProducerConfig()
	if err != nil {
		return nil, err
	}
	return producer.New(cfg)
}

func (c *Client) Consumer() (*Consumer, error) {
	cfg, err := c.config.ConsumerConfig()
	if err != nil {
		return nil, err
	}
	return consumer.New(cfg)
}

func NewHandler(topic string, fn func(ctx context.Context, msg Message) error) Handler {
	return consumer.NewHandler(topic, fn)
}

func JSONHandler[T any](topic string, fn func(ctx context.Context, data T) error) Handler {
	return consumer.JSONHandler(topic, fn)
}

func WithKey(key []byte) ProducerOption {
	return producer.WithKey(key)
}

func WithHeaders(headers ...Header) ProducerOption {
	return producer.WithHeaders(headers...)
}

func WithTime(t time.Time) ProducerOption {
	return producer.WithTime(t)
}
