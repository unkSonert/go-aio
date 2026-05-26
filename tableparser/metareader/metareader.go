package metareader

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/unkSonert/go-aio/tableparser/config"
)

type TagsMap map[string]string
type TagsMaps []TagsMap

const LookupTag = config.DefaultLookupTag

var (
	ErrInvalidParam  = errors.New("metareader: destination must be a pointer to a slice of structs with lookup tags")
	ErrNotEnoughRows = errors.New("metareader: rows must contain a header and at least one data row")
	ErrInvalidHeader = errors.New("metareader: row has more cells than the header")
)

type Option = config.Option
type Options = config.Options

func ReadAll(dest any, rows [][]string, opts ...Option) (error, []error) {
	if len(rows) <= 1 {
		return ErrNotEnoughRows, nil
	}

	options := config.NewOptions(opts...)
	header := rows[0]
	tagsMaps := make(TagsMaps, 0, len(rows)-1)

	for i := 1; i < len(rows); i++ {
		tagsMaps = append(tagsMaps, rowToTags(header, rows[i], options))
	}

	err, elementErrs := parseAllTags(dest, tagsMaps, options)
	if err != nil {
		return err, nil
	}

	return nil, elementErrs
}

func rowToTags(header, row []string, options Options) TagsMap {
	result := TagsMap{}

	if len(header) < len(row) {
		row = row[:len(header)]
	}

	for i := 0; i < len(row); i++ {
		value := row[i]
		if value == "" {
			continue
		}

		key := normalizeName(header[i], options)
		if key == "" {
			continue
		}
		result[key] = value
	}

	return result
}

func parseAllTags(targetArr any, tagsMaps TagsMaps, options Options) (error, []error) {
	var elementErrs []error
	arrayBaseType := reflect.TypeOf(targetArr)

	if arrayBaseType == nil || arrayBaseType.Kind() != reflect.Pointer {
		return ErrInvalidParam, elementErrs
	}

	arrayPointer := reflect.ValueOf(targetArr)
	if arrayPointer.IsNil() {
		return ErrInvalidParam, elementErrs
	}

	array := arrayPointer.Elem()
	typeArray := array.Type()

	if typeArray.Kind() != reflect.Slice || !array.CanSet() {
		return ErrInvalidParam, elementErrs
	}

	typeElement := typeArray.Elem()
	if typeElement.Kind() != reflect.Struct {
		return ErrInvalidParam, elementErrs
	}

	elementErrs = make([]error, 0, len(tagsMaps))
	for _, tagsMap := range tagsMaps {
		newElement, err := newElementByTagMap(typeElement, tagsMap, options)

		elementErrs = append(elementErrs, err)
		array.Set(reflect.Append(array, newElement))
	}

	return nil, elementErrs
}

func newElementByTagMap(elementType reflect.Type, tagsMap TagsMap, options Options) (reflect.Value, error) {
	newElement := reflect.Indirect(reflect.New(elementType))
	var errStrings []string

	for i := 0; i < elementType.NumField(); i++ {
		elementField := newElement.Field(i)
		elementFieldInfo := elementType.Field(i)
		tagValue, tagFound := elementFieldInfo.Tag.Lookup(options.LookupTag)
		if !tagFound {
			continue
		}

		tagName := normalizeName(tagValue, options)
		if tagName == "" {
			continue
		}

		value, ok := tagsMap[tagName]
		if !ok {
			if elementFieldInfo.Type.Kind() != reflect.Pointer {
				errStrings = append(errStrings, "missing required field: "+tagValue)
			}
			continue
		}

		if !elementField.CanSet() {
			errStrings = append(errStrings, fmt.Sprintf("%s: field cannot be set", elementFieldInfo.Name))
			continue
		}
		if err := setFieldValue(elementField, value); err != nil {
			errStrings = append(errStrings, fmt.Sprintf("%s: %v", elementFieldInfo.Name, err))
		}
	}

	var err error
	if len(errStrings) != 0 {
		err = errors.New(strings.Join(errStrings, ";"))
	}

	return newElement, err
}

func setFieldValue(field reflect.Value, value string) error {
	if field.Kind() == reflect.Pointer {
		element := reflect.New(field.Type().Elem())
		if err := setScalarValue(element.Elem(), value); err != nil {
			return err
		}
		field.Set(element)
		return nil
	}

	return setScalarValue(field, value)
}

func setScalarValue(field reflect.Value, value string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		intValue, err := strconv.ParseInt(value, 10, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetInt(intValue)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uintValue, err := strconv.ParseUint(value, 10, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetUint(uintValue)
	case reflect.Bool:
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		field.SetBool(boolValue)
	case reflect.Float32, reflect.Float64:
		floatValue, err := strconv.ParseFloat(value, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetFloat(floatValue)
	default:
		return fmt.Errorf("unsupported field type %s", field.Type())
	}

	return nil
}

func normalizeName(value string, options Options) string {
	value = strings.TrimSpace(value)
	if options.CaseSensitiveHeader {
		return value
	}
	return strings.ToLower(value)
}
