package xlsx

import "github.com/unkSonert/go-aio/tableparser/config"

type Option = config.Option
type Options = config.Options

func WithTag(tag string) Option {
	return config.WithTag(tag)
}

func WithCaseSensitiveHeader(enabled bool) Option {
	return config.WithCaseSensitiveHeader(enabled)
}
