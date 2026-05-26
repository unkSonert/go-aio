package config

const (
	DefaultLookupTag = "rowName"
	DefaultCSVComma  = ';'
)

type Options struct {
	LookupTag           string
	CaseSensitiveHeader bool
	CSVComma            rune
}

type Option func(*Options)

func DefaultOptions() Options {
	return Options{
		LookupTag: DefaultLookupTag,
		CSVComma:  DefaultCSVComma,
	}
}

func NewOptions(opts ...Option) Options {
	options := DefaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	if options.LookupTag == "" {
		options.LookupTag = DefaultLookupTag
	}
	if options.CSVComma == 0 {
		options.CSVComma = DefaultCSVComma
	}
	return options
}

func WithTag(tag string) Option {
	return func(options *Options) {
		options.LookupTag = tag
	}
}

func WithCaseSensitiveHeader(enabled bool) Option {
	return func(options *Options) {
		options.CaseSensitiveHeader = enabled
	}
}

func WithCSVComma(comma rune) Option {
	return func(options *Options) {
		options.CSVComma = comma
	}
}
