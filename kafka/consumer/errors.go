package consumer

import "errors"

var (
	ErrAlreadyRunning   = errors.New("kafkax/consumer: consumer is already running")
	ErrHandlerRequired  = errors.New("kafkax/consumer: handler is required")
	ErrNoHandlers       = errors.New("kafkax/consumer: at least one topic handler is required")
	ErrStopTimeout      = errors.New("kafkax/consumer: stop timeout")
	ErrTopicRequired    = errors.New("kafkax/consumer: topic is required")
	ErrDuplicateHandler = errors.New("kafkax/consumer: handler for topic is already registered")
)
