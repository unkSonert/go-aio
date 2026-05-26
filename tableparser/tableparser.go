package tableparser

import (
	"errors"
	"io"
	"path"
	"strings"

	"github.com/unkSonert/go-aio/tableparser/config"
	"github.com/unkSonert/go-aio/tableparser/csv"
	"github.com/unkSonert/go-aio/tableparser/metareader"
	"github.com/unkSonert/go-aio/tableparser/xlsx"
)

const (
	CSVExt  = ".csv"
	XLSXExt = ".xlsx"
	XLSExt  = ".xls"

	LookupTag = config.DefaultLookupTag
)

var ErrUnknownFormat = errors.New("tableparser: unknown table format")

type Options = config.Options
type Option = config.Option

func ReadAll(fileName string, reader io.Reader, dest any, opts ...Option) (totalRows int, err error, elementsErrs []error) {
	ext := strings.ToLower(path.Ext(fileName))

	switch ext {
	case CSVExt:
		return ReadCSV(reader, dest, opts...)
	case XLSXExt, XLSExt:
		return ReadXLSX(reader, dest, opts...)
	default:
		return 0, ErrUnknownFormat, nil
	}
}

func ReadCSV(reader io.Reader, dest any, opts ...Option) (totalRows int, err error, elementsErrs []error) {
	return csv.ReadAll(reader, dest, opts...)
}

func ReadXLSX(reader io.Reader, dest any, opts ...Option) (totalRows int, err error, elementsErrs []error) {
	return xlsx.ReadAll(reader, dest, opts...)
}

func ReadRows(dest any, rows [][]string, opts ...Option) (error, []error) {
	return metareader.ReadAll(dest, rows, opts...)
}

func WithTag(tag string) Option {
	return config.WithTag(tag)
}

func WithCaseSensitiveHeader(enabled bool) Option {
	return config.WithCaseSensitiveHeader(enabled)
}

func WithCSVComma(comma rune) Option {
	return config.WithCSVComma(comma)
}
