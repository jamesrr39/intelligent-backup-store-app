package csvx

import (
	"fmt"
	"reflect"
)

func traverseFields(target interface{}, createMissingStructs bool, fn func(fieldCsvTag string, field reflect.Value /*field reflect.StructField*/) error) error {
	rv := reflect.ValueOf(target)
	rt := reflect.TypeOf(target)
	if rt.Kind() == reflect.Pointer {
		rv = rv.Elem()
		rt = rt.Elem()
	}

	for i := 0; i < rt.NumField(); i++ {
		fieldV := rv.Field(i)
		fieldT := rt.Field(i)

		const csvTagName = "csv"

		csvTag := fieldT.Tag.Get(csvTagName)
		if csvTag != "" {
			err := fn(csvTag, fieldV)
			if err != nil {
				return err
			}
		}

		fieldUnderlyingKind := getUnderlyingObject(fieldV).Kind()

		// reflect.Invalid for a anonymous pointer field to another struct.
		// Can't get the information about that struct so instead insist it is already set when sending in the `target`.
		if fieldUnderlyingKind == reflect.Invalid {
			return fmt.Errorf("csvx: invalid type found for field %q. If an anonymous struct, please ensure this field is already created before passing in to decode", fieldT.Name)
		}

		// if field is a struct, go into that struct and look for tags there
		if fieldUnderlyingKind == reflect.Struct {
			if csvTag != "" {
				return fmt.Errorf("csvx: %q tag on anonymous field not supported", csvTagName)
			}

			fieldOrCreatedObjectV := fieldV
			if createMissingStructs {
				// create a settable field. Links to understand this concept:
				// https://github.com/robertkrimen/otto/issues/83
				// https: //go.dev/blog/laws-of-reflection
				fieldOrCreatedObjectV = reflect.New(reflect.Indirect(reflect.ValueOf(fieldV.Interface())).Type())
			}

			err := traverseFields(fieldOrCreatedObjectV.Interface(), createMissingStructs, fn)
			if err != nil {
				return err
			}

			if createMissingStructs {
				// set the struct.
				// Do this after filling in the values, as we need to send pointer fields to `traverseFields` so they are retained after function exit,
				// but non-pointer fields must be converted to their plain type
				if fieldV.Kind() != reflect.Pointer {
					fieldOrCreatedObjectV = fieldOrCreatedObjectV.Elem()
				}

				fieldV.Set(fieldOrCreatedObjectV)
			}
		}
	}

	return nil
}

func getUnderlyingObject(rv reflect.Value) reflect.Value {
	if rv.Kind() == reflect.Pointer {
		return rv.Elem()
	}

	return rv
}
