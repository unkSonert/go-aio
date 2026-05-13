package producer

import "errors"

var (
	ErrClosed        = errors.New("kafkax/producer: producer is closed")
	ErrTopicRequired = errors.New("kafkax/producer: topic is required")
)
