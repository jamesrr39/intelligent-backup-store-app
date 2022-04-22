package csvx

import (
	"fmt"
	"reflect"
	"strconv"
)

type Encoder struct {
	FieldsMap                   map[string]int
	FloatFmt                    byte
	NullText                    string
	BoolTrueText, BoolFalseText string
}

func NewEncoder(fields []string) *Encoder {
	fieldsMap := make(map[string]int)
	for i, field := range fields {
		fieldsMap[field] = i
	}

	return &Encoder{fieldsMap, 'f', "null", "true", "false"}
}

func (e *Encoder) Encode(target interface{}) ([]string, error) {
	fieldsMapLen := len(e.FieldsMap)

	if fieldsMapLen == 0 {
		return nil, fmt.Errorf("no fields selected for encoding")
	}

	records := make([]string, fieldsMapLen)

	onFieldFound := func(fieldCsvTag string, field reflect.Value) error {
		var err error

		idx, ok := e.FieldsMap[fieldCsvTag]
		if !ok {
			// field not requested for scanning
			return nil
		}

		records[idx], err = e.toString(field)
		if err != nil {
			return fmt.Errorf("csvx: error getting string from field. Field: %q, field index: %d. Error: %s", fieldCsvTag, idx, err)
		}

		return nil
	}

	err := traverseFields(target, false, onFieldFound)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (e *Encoder) toString(field reflect.Value) (string, error) {
	kind := field.Kind()
	if kind == reflect.Pointer {
		if field.IsNil() {
			return e.NullText, nil
		}

		return e.toString(field.Elem())
	}

	switch kind {
	case reflect.String:
		return field.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(field.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(field.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(field.Float(), e.FloatFmt, -1, 64), nil
	case reflect.Bool:
		val := field.Bool()
		if val {
			return e.BoolTrueText, nil
		}
		return e.BoolFalseText, nil
	default:
		return "", fmt.Errorf("toString not implemented for kind %q", kind)
	}

}
