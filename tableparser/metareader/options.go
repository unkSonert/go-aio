package metareader

import "github.com/unkSonert/go-aio/tableparser/config"

func WithTag(tag string) Option {
	return config.WithTag(tag)
}

func WithCaseSensitiveHeader(enabled bool) Option {
	return config.WithCaseSensitiveHeader(enabled)
}
