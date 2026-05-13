package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	DefaultBroker        = "localhost:9092"
	DefaultClientID      = "kafkax"
	DialTimeout          = 10 * time.Second
	ProducerRequiredAcks = int(kafka.RequireAll)
	ProducerMaxAttempts  = 10
	ConsumerStartOffset  = kafka.FirstOffset
)

var (
	ErrBrokersRequired = errors.New("kafkax/config: at least one kafka broker is required")
	ErrGroupIDRequired = errors.New("kafkax/config: consumer group id is required")
	ErrInvalidCACert   = errors.New("kafkax/config: invalid TLS CA certificate PEM")
)

type Config struct {
	Brokers     []string `env:"KAFKA_BROKERS" envDefault:"localhost:9092" envSeparator:"," yaml:"brokers"`
	ClientID    string   `env:"KAFKA_CLIENT_ID" envDefault:"kafkax" yaml:"client_id"`
	GroupID     string   `env:"KAFKA_GROUP_ID" yaml:"group_id"`
	TopicPrefix string   `env:"KAFKA_TOPIC_PREFIX" yaml:"topic_prefix"`
	TLS         TLS      `yaml:"tls"`
	SASL        SASL     `yaml:"sasl"`
	DLQ         DLQ      `yaml:"dlq"`

	TLSConfig     *tls.Config    `yaml:"-"`
	SASLMechanism sasl.Mechanism `yaml:"-"`
}

type TLS struct {
	Enabled            bool   `env:"KAFKA_TLS_ENABLED" envDefault:"false" yaml:"enabled"`
	ServerName         string `env:"KAFKA_TLS_SERVER_NAME" yaml:"server_name"`
	InsecureSkipVerify bool   `env:"KAFKA_TLS_INSECURE_SKIP_VERIFY" envDefault:"false" yaml:"insecure_skip_verify"`
	CACert             []byte `env:"KAFKA_TLS_CA_CERT" yaml:"ca_cert"`
}

type SASL struct {
	Mechanism string `env:"KAFKA_SASL_MECHANISM" yaml:"mechanism"`
	Username  string `env:"KAFKA_SASL_USERNAME" yaml:"username"`
	Password  string `env:"KAFKA_SASL_PASSWORD" yaml:"password"`
}

type DLQ struct {
	Topic       string `env:"KAFKA_DLQ_TOPIC" yaml:"topic"`
	TopicSuffix string `env:"KAFKA_DLQ_TOPIC_SUFFIX" yaml:"topic_suffix"`
}

func (d DLQ) Enabled() bool {
	return d.Topic != "" || d.TopicSuffix != ""
}

func (d DLQ) TopicFor(sourceTopic string) string {
	if d.Topic != "" {
		return d.Topic
	}
	return sourceTopic + d.TopicSuffix
}

type Kafka struct {
	Brokers     []string
	ClientID    string
	TopicPrefix string
	TLS         *tls.Config
	SASL        sasl.Mechanism
}

type Producer struct {
	Kafka
}

type Consumer struct {
	Kafka
	GroupID string
	DLQ     DLQ
}

func New(brokers ...string) Config {
	return Config{Brokers: brokers}
}

func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrBrokersRequired
	}
	if _, err := c.saslMechanism(); err != nil {
		return err
	}
	if _, err := c.tlsConfig(); err != nil {
		return err
	}
	return nil
}

func (c Config) WithDefaults() Config {
	if len(c.Brokers) == 0 {
		c.Brokers = []string{DefaultBroker}
	}
	if c.ClientID == "" {
		c.ClientID = DefaultClientID
	}
	return c
}

func (c Config) KafkaConfig() (Kafka, error) {
	c = c.WithDefaults()
	mechanism, err := c.saslMechanism()
	if err != nil {
		return Kafka{}, err
	}
	tlsCfg, err := c.tlsConfig()
	if err != nil {
		return Kafka{}, err
	}
	return Kafka{
		Brokers:     c.Brokers,
		ClientID:    c.ClientID,
		TopicPrefix: c.TopicPrefix,
		TLS:         tlsCfg,
		SASL:        mechanism,
	}, nil
}

func (c Config) ProducerConfig() (Producer, error) {
	k, err := c.KafkaConfig()
	if err != nil {
		return Producer{}, err
	}
	return Producer{Kafka: k}, nil
}

func (c Config) ConsumerConfig() (Consumer, error) {
	k, err := c.KafkaConfig()
	if err != nil {
		return Consumer{}, err
	}
	if c.GroupID == "" {
		return Consumer{}, ErrGroupIDRequired
	}
	return Consumer{Kafka: k, GroupID: c.GroupID, DLQ: c.DLQ}, nil
}

func (c Kafka) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrBrokersRequired
	}
	return nil
}

func (c Kafka) Dialer() *kafka.Dialer {
	clientID := c.ClientID
	if clientID == "" {
		clientID = DefaultClientID
	}
	return &kafka.Dialer{
		ClientID:      clientID,
		Timeout:       DialTimeout,
		TLS:           c.TLS,
		SASLMechanism: c.SASL,
	}
}

func (c Producer) Validate() error {
	return c.Kafka.Validate()
}

func (c Producer) WriterConfig() kafka.WriterConfig {
	return kafka.WriterConfig{
		Brokers:      c.Brokers,
		Dialer:       c.Dialer(),
		RequiredAcks: ProducerRequiredAcks,
		MaxAttempts:  ProducerMaxAttempts,
	}
}

func (c Consumer) Validate() error {
	if err := c.Kafka.Validate(); err != nil {
		return err
	}
	if c.GroupID == "" {
		return ErrGroupIDRequired
	}
	return nil
}

func (c Consumer) ReaderConfig(topics []string) kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:        c.Brokers,
		GroupID:        c.GroupID,
		GroupTopics:    topics,
		Dialer:         c.Dialer(),
		CommitInterval: 0,
		StartOffset:    ConsumerStartOffset,
	}
}

func (c Config) tlsConfig() (*tls.Config, error) {
	if c.TLSConfig != nil {
		return c.TLSConfig, nil
	}
	if !c.TLS.Enabled {
		return nil, nil
	}
	cfg := &tls.Config{
		ServerName:         c.TLS.ServerName,
		InsecureSkipVerify: c.TLS.InsecureSkipVerify,
	}
	if len(c.TLS.CACert) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(c.TLS.CACert) {
			return nil, ErrInvalidCACert
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func (c Config) saslMechanism() (sasl.Mechanism, error) {
	if c.SASLMechanism != nil {
		return c.SASLMechanism, nil
	}
	switch normalizeMechanism(c.SASL.Mechanism) {
	case "", "NONE":
		return nil, nil
	case "PLAIN":
		return plain.Mechanism{Username: c.SASL.Username, Password: c.SASL.Password}, nil
	case "SCRAMSHA256":
		return scram.Mechanism(scram.SHA256, c.SASL.Username, c.SASL.Password)
	case "SCRAMSHA512":
		return scram.Mechanism(scram.SHA512, c.SASL.Username, c.SASL.Password)
	default:
		return nil, fmt.Errorf("kafkax/config: unsupported sasl mechanism %q", c.SASL.Mechanism)
	}
}

func normalizeMechanism(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "-", "")
	value = strings.ReplaceAll(value, "_", "")
	value = strings.ReplaceAll(value, " ", "")
	return value
}
