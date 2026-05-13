package consumer

import (
	"encoding/json"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Message struct {
	Data          json.RawMessage
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Headers       []kafka.Header
	Time          time.Time
	Attempt       int
}

func (m Message) Decode(v any) error {
	return json.Unmarshal(m.Data, v)
}

func (m Message) Header(key string) ([]byte, bool) {
	for _, h := range m.Headers {
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}

func fromKafkaMessage(msg kafka.Message) Message {
	return Message{
		Data:          json.RawMessage(msg.Value),
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		HighWaterMark: msg.HighWaterMark,
		Key:           msg.Key,
		Headers:       msg.Headers,
		Time:          msg.Time,
	}
}
