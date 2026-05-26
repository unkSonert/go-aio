package csv

import (
	"encoding/csv"
	"io"

	"github.com/unkSonert/go-aio/tableparser/config"
	"github.com/unkSonert/go-aio/tableparser/metareader"
)

func ReadAll(reader io.Reader, dest any, opts ...Option) (int, error, []error) {
	options := config.NewOptions(opts...)
	r := csv.NewReader(reader)
	r.Comma = options.CSVComma
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return 0, err, nil
	}

	err, elementsErrs := metareader.ReadAll(dest, rows, opts...)

	return dataRowsCount(rows), err, elementsErrs
}

func ReadFile(reader io.Reader, dest any, opts ...Option) (int, error, []error) {
	return ReadAll(reader, dest, opts...)
}

func dataRowsCount(rows [][]string) int {
	if len(rows) == 0 {
		return 0
	}
	return len(rows) - 1
}
