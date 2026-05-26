package xlsx

import (
	"io"

	"github.com/unkSonert/go-aio/tableparser/metareader"
	"github.com/xuri/excelize/v2"
)

func ReadAll(reader io.Reader, dest any, opts ...Option) (int, error, []error) {
	file, err := excelize.OpenReader(reader)
	if err != nil {
		return 0, err, nil
	}
	defer file.Close()

	rows, err := file.GetRows(file.GetSheetName(file.GetActiveSheetIndex()))
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
