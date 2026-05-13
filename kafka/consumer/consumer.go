package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/unkSonert/go-aio/kafka/config"
	"github.com/unkSonert/go-aio/kafka/producer"
)

const (
	handlerTimeout     = 30 * time.Second
	maxHandlerAttempts = 3
	retryBackoff       = 1 * time.Second
	commitTimeout      = 10 * time.Second
	dlqPublishTimeout  = 10 * time.Second
)

type Consumer struct {
	config config.Consumer

	mu       sync.RWMutex
	handlers map[string]Handler
	running  bool
	reader   *kafka.Reader
	dlq      *producer.Producer
	cancel   context.CancelFunc
	done     chan struct{}
}

type deadLetterData struct {
	OriginalTopic     string          `json:"original_topic"`
	OriginalPartition int             `json:"original_partition"`
	OriginalOffset    int64           `json:"original_offset"`
	Payload           json.RawMessage `json:"payload,omitempty"`
	Error             string          `json:"error"`
	Attempts          int             `json:"attempts"`
	FailedAt          time.Time       `json:"failed_at"`
}

func New(cfg config.Consumer) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Consumer{
		config:   cfg,
		handlers: make(map[string]Handler),
	}, nil
}

func (c *Consumer) Handle(handlers ...Handler) error {
	if len(handlers) == 0 {
		return ErrHandlerRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return ErrAlreadyRunning
	}

	seen := make(map[string]struct{}, len(handlers))
	for _, handler := range handlers {
		if handler == nil {
			return ErrHandlerRequired
		}
		topic := handler.Topic()
		if topic == "" {
			return ErrTopicRequired
		}
		if _, exists := c.handlers[topic]; exists {
			return fmt.Errorf("%w: %q", ErrDuplicateHandler, topic)
		}
		if _, exists := seen[topic]; exists {
			return fmt.Errorf("%w: %q", ErrDuplicateHandler, topic)
		}
		seen[topic] = struct{}{}
	}
	for _, handler := range handlers {
		c.handlers[handler.Topic()] = handler
	}
	return nil
}

func (c *Consumer) Topics() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return topicsFromHandlers(c.handlers)
}

func (c *Consumer) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		cancel()
		return ErrAlreadyRunning
	}
	if len(c.handlers) == 0 {
		c.mu.Unlock()
		cancel()
		return ErrNoHandlers
	}
	c.running = true
	c.cancel = cancel
	c.done = done
	topics := topicsFromHandlers(c.handlers)
	wireTopics := prefixTopics(c.config.TopicPrefix, topics)
	handlers := make(map[string]Handler, len(c.handlers))
	for k, v := range c.handlers {
		handlers[k] = v
	}
	c.mu.Unlock()

	reader := kafka.NewReader(c.config.ReaderConfig(wireTopics))
	var dlq *producer.Producer
	if c.config.DLQ.Enabled() {
		var err error
		dlq, err = producer.New(config.Producer{Kafka: c.config.Kafka})
		if err != nil {
			_ = reader.Close()
			c.finishRun(done)
			return err
		}
	}

	c.mu.Lock()
	c.reader = reader
	c.dlq = dlq
	c.mu.Unlock()

	defer func() {
		_ = reader.Close()
		if dlq != nil {
			_ = dlq.Close()
		}
		cancel()
		c.finishRun(done)
	}()

	for {
		msg, err := reader.FetchMessage(runCtx)
		if err != nil {
			if errors.Is(err, io.EOF) || runCtx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafkax/consumer: fetch message: %w", err)
		}
		if err := c.processMessage(runCtx, reader, dlq, handlers, msg); err != nil {
			if ctxErr := runCtx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				return nil
			}
			return err
		}
	}
}

func (c *Consumer) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	c.mu.Lock()
	reader := c.reader
	dlq := c.dlq
	cancel := c.cancel
	done := c.done
	c.mu.Unlock()

	var err error
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			err = fmt.Errorf("%w: %v", ErrStopTimeout, ctx.Err())
		}
	}
	if reader != nil {
		if readerErr := reader.Close(); err == nil {
			err = readerErr
		}
	}
	if dlq != nil {
		if dlqErr := dlq.Close(); err == nil {
			err = dlqErr
		}
	}
	return err
}

func (c *Consumer) Close() error {
	return c.Stop(context.Background())
}

func (c *Consumer) finishRun(done chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.running = false
	c.reader = nil
	c.dlq = nil
	if c.done == done {
		c.done = nil
	}
	c.cancel = nil
	close(done)
}

func (c *Consumer) processMessage(ctx context.Context, reader *kafka.Reader, dlq *producer.Producer, handlers map[string]Handler, kafkaMsg kafka.Message) error {
	logicalTopic := strings.TrimPrefix(kafkaMsg.Topic, c.config.TopicPrefix)
	msg := fromKafkaMessage(kafkaMsg)
	msg.Topic = logicalTopic

	handler, ok := handlers[logicalTopic]
	if !ok {
		failure := fmt.Errorf("kafkax/consumer: no handler for topic %q partition %d offset %d", kafkaMsg.Topic, kafkaMsg.Partition, kafkaMsg.Offset)
		return c.handleFailedMessage(ctx, reader, dlq, kafkaMsg, failure)
	}

	if err := c.handleWithRetries(ctx, handler, msg); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		failure := fmt.Errorf("kafkax/consumer: handler failed for topic %q partition %d offset %d: %w", kafkaMsg.Topic, kafkaMsg.Partition, kafkaMsg.Offset, err)
		return c.handleFailedMessage(ctx, reader, dlq, kafkaMsg, failure)
	}
	return c.commitMessage(ctx, reader, kafkaMsg)
}

func (c *Consumer) handleWithRetries(ctx context.Context, handler Handler, msg Message) error {
	var lastErr error
	for attempt := 1; attempt <= maxHandlerAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		attemptMsg := msg
		attemptMsg.Attempt = attempt
		handlerCtx, cancel := context.WithTimeout(ctx, handlerTimeout)

		lastErr = handler.Handle(handlerCtx, attemptMsg)
		cancel()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < maxHandlerAttempts {
			if err := sleepContext(ctx, retryBackoff); err != nil {
				return err
			}
		}
	}
	return lastErr
}

func (c *Consumer) handleFailedMessage(ctx context.Context, reader *kafka.Reader, dlq *producer.Producer, kafkaMsg kafka.Message, failure error) error {
	if dlq != nil && c.config.DLQ.Enabled() {
		if err := c.publishDeadLetter(ctx, dlq, kafkaMsg, failure); err != nil {
			return fmt.Errorf("kafkax/consumer: publish dead letter for topic %q partition %d offset %d: %w", kafkaMsg.Topic, kafkaMsg.Partition, kafkaMsg.Offset, err)
		}
		return c.commitMessage(ctx, reader, kafkaMsg)
	}

	log.Printf("%v: skipping message", failure)
	return c.commitMessage(ctx, reader, kafkaMsg)
}

func (c *Consumer) publishDeadLetter(ctx context.Context, dlq *producer.Producer, kafkaMsg kafka.Message, failure error) error {
	publishCtx, cancel := context.WithTimeout(ctx, dlqPublishTimeout)
	defer cancel()

	data := deadLetterData{
		OriginalTopic:     kafkaMsg.Topic,
		OriginalPartition: kafkaMsg.Partition,
		OriginalOffset:    kafkaMsg.Offset,
		Payload:           json.RawMessage(kafkaMsg.Value),
		Error:             failure.Error(),
		Attempts:          maxHandlerAttempts,
		FailedAt:          time.Now().UTC(),
	}

	headers := make([]kafka.Header, 0, len(kafkaMsg.Headers)+4)
	headers = append(headers, kafkaMsg.Headers...)
	headers = append(headers,
		kafka.Header{Key: "kafkax-original-topic", Value: []byte(kafkaMsg.Topic)},
		kafka.Header{Key: "kafkax-original-partition", Value: []byte(strconv.Itoa(kafkaMsg.Partition))},
		kafka.Header{Key: "kafkax-original-offset", Value: []byte(strconv.FormatInt(kafkaMsg.Offset, 10))},
		kafka.Header{Key: "kafkax-error", Value: []byte(failure.Error())},
	)
	logicalTopic := strings.TrimPrefix(kafkaMsg.Topic, c.config.TopicPrefix)
	return dlq.Publish(
		publishCtx,
		c.config.DLQ.TopicFor(logicalTopic),
		data,
		producer.WithKey(kafkaMsg.Key),
		producer.WithHeaders(headers...),
	)
}

func (c *Consumer) commitMessage(ctx context.Context, reader *kafka.Reader, msg kafka.Message) error {
	commitCtx, cancel := context.WithTimeout(ctx, commitTimeout)
	defer cancel()
	if err := reader.CommitMessages(commitCtx, msg); err != nil {
		return fmt.Errorf("kafkax/consumer: commit topic %q partition %d offset %d: %w", msg.Topic, msg.Partition, msg.Offset, err)
	}
	return nil
}

func topicsFromHandlers(handlers map[string]Handler) []string {
	topics := make([]string, 0, len(handlers))
	for topic := range handlers {
		topics = append(topics, topic)
	}
	return topics
}

func prefixTopics(prefix string, topics []string) []string {
	if prefix == "" {
		return topics
	}
	out := make([]string, len(topics))
	for i, t := range topics {
		out[i] = prefix + t
	}
	return out
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
