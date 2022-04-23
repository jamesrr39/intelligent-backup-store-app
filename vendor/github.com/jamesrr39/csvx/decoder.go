package csvx

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type Decoder struct {
	FieldsMap                   map[string]int
	NullText                    string
	BoolTrueText, BoolFalseText []string
}

func NewDecoder(fields []string) *Decoder {
	fieldsMap := make(map[string]int)
	for i, field := range fields {
		fieldsMap[field] = i
	}

	return &Decoder{
		FieldsMap:     fieldsMap,
		NullText:      "null",
		BoolTrueText:  []string{"true", "yes", "1", "1.0"},
		BoolFalseText: []string{"false", "no", "0", "0.0"},
	}
}

func (d *Decoder) Decode(values []string, target interface{}) error {
	// check target is a non-nil pointer
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("csvx: unmarshal target must be a non-nil pointer, but got %s", rv.Type())
	}

	if len(values) != len(d.FieldsMap) {
		return fmt.Errorf("csvx: amount of fields (%d) does not match amount of values passed in (%d)", len(d.FieldsMap), len(values))
	}

	onFieldFound := func(fieldCsvTag string, field reflect.Value) error {
		isPtr := field.Kind() == reflect.Pointer

		fieldIdx, ok := d.FieldsMap[fieldCsvTag]
		if !ok {
			return fmt.Errorf("csvx: field not found: %q", fieldCsvTag)
		}

		valueStr := values[fieldIdx]

		err := d.setField(field, field.Kind(), valueStr, isPtr)
		if err != nil {
			return err
		}

		return nil
	}

	err := traverseFields(target, true, onFieldFound)
	if err != nil {
		return err
	}

	return nil
}

func (d *Decoder) setField(field reflect.Value, fieldKind reflect.Kind, valueStr string, isPtr bool) error {
	if !field.CanSet() {
		return fmt.Errorf("cannot set field: %q", field.Type().Name())
	}
	switch fieldKind {
	case reflect.String:
		if isPtr {
			field.Set(reflect.ValueOf(&valueStr))
		} else {
			field.SetString(valueStr)
		}
	case reflect.Int:
		val, err := strconv.Atoi(valueStr)
		if err != nil {
			return err
		}

		if isPtr {
			field.Set(reflect.ValueOf(&val))
		} else {
			field.SetInt(int64(val))
		}
	case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		bitSize, err := bitSizeFromKind(fieldKind)
		if err != nil {
			return err
		}

		val, err := strconv.ParseInt(valueStr, 10, bitSize)
		if err != nil {
			return err
		}

		if isPtr {
			field.Set(reflect.ValueOf(&val))
		} else {
			field.SetInt(int64(val))
		}
	case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		bitSize, err := bitSizeFromKind(fieldKind)
		if err != nil {
			return err
		}

		val, err := strconv.ParseUint(valueStr, 10, bitSize)
		if err != nil {
			return err
		}

		if isPtr {
			field.Set(reflect.ValueOf(&val))
		} else {
			field.SetUint(val)
		}
	case reflect.Float64, reflect.Float32:
		bitSize, err := bitSizeFromKind(fieldKind)
		if err != nil {
			return err
		}

		val, err := strconv.ParseFloat(valueStr, bitSize)
		if err != nil {
			return err
		}

		if isPtr {
			field.Set(reflect.ValueOf(&val))
		} else {
			field.SetFloat(val)
		}
	case reflect.Bool:
		val, err := d.boolValueFromStr(valueStr)
		if err != nil {
			return err
		}
		if isPtr {
			field.Set(reflect.ValueOf(&val))
		} else {
			field.SetBool(val)
		}
	case reflect.Pointer:
		if valueStr == "" || valueStr == d.NullText {
			// leave field nil
			return nil
		}

		err := d.setField(field, field.Type().Elem().Kind(), valueStr, true)
		if err != nil {
			return err
		}
	case reflect.Struct:
		val := field.Interface()

		var isUsingPtrType bool
		v, ok := val.(encoding.TextUnmarshaler)
		if !ok {
			// see if encoding.TextUnmarshaler is implemented on the pointer type of this struct. If it is, use that.
			valPtr := reflect.New(reflect.Indirect(reflect.ValueOf(val)).Type()).Interface()
			v, ok = valPtr.(encoding.TextUnmarshaler)
			if !ok {
				return fmt.Errorf("decoding not implemented for kind %q (type: %s). Encoding.TextUnmarshaler not implemented, implement it to unmarshal this field", fieldKind, field.Type().String())
			}

			isUsingPtrType = true
		}

		err := v.UnmarshalText([]byte(valueStr))
		if err != nil {
			return err
		}

		// get the reflect.Value to set on the field. If we have used a pointer type, extract the plain struct type from that.
		objToSet := reflect.ValueOf(v)
		if isUsingPtrType {
			objToSet = objToSet.Elem()
		}

		field.Set(objToSet)

		return nil
	default:
		return fmt.Errorf("field type not implemented: %s", fieldKind)
	}

	return nil
}

func (d *Decoder) boolValueFromStr(valueStr string) (bool, error) {
	valToLower := strings.ToLower(valueStr)
	if stringSliceContains(d.BoolTrueText, valToLower) {
		return true, nil
	}
	if stringSliceContains(d.BoolFalseText, valToLower) {
		return false, nil
	}

	return false, fmt.Errorf("couldn't understand value that should be a boolean field")
}

func bitSizeFromKind(kind reflect.Kind) (int, error) {
	switch kind {
	case reflect.Int64, reflect.Float64:
		return 64, nil
	case reflect.Int32, reflect.Float32:
		return 32, nil
	case reflect.Int16:
		return 16, nil
	case reflect.Int8:
		return 8, nil
	}

	return 0, fmt.Errorf("kind not handled: %s", kind)
}

func stringSliceContains(searchingIn []string, lookingFor string) bool {
	for _, item := range searchingIn {
		if item == lookingFor {
			return true
		}
	}

	return false
}
