package csvx

import (
	"fmt"
	"reflect"
	"strconv"
)

type Encoder struct {
	Fields                      []string
	FloatFmt                    byte
	NullText                    string
	BoolTrueText, BoolFalseText string
}

func NewEncoderWithDefaultOpts(fields []string) *Encoder {
	return &Encoder{fields, 'f', "null", "true", "false"}
}

func (e *Encoder) Encode(target interface{}) ([]string, error) {
	var err error

	if len(e.Fields) == 0 {
		return nil, fmt.Errorf("no fields selected for encoding")
	}

	rv := reflect.ValueOf(target)

	elem := reflect.TypeOf(target)

	fieldIndexByName := buildFieldIndexByName(rv, elem)

	values := make([]string, len(fieldIndexByName))

	for i, fieldName := range e.Fields {
		fieldIndex, ok := fieldIndexByName[fieldName]
		if !ok {
			return nil, fmt.Errorf("csv: could not find field %q in struct. Make sure the tag 'csv' is set.", fieldName)
		}

		field := getUnderlyingObject(rv).Field(fieldIndex)

		values[i], err = e.toString(field)
		if err != nil {
			return nil, fmt.Errorf("csvx: error getting string from field. Field: %v, field index: %d. Error: %s", field, i, err)
		}
	}

	return values, nil
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
