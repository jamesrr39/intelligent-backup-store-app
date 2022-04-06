package csvx

import "reflect"

func buildFieldIndexByName(rv reflect.Value, elem reflect.Type) map[string]int {
	obj := getUnderlyingObject(rv)
	elem = obj.Type()

	fieldIndexByName := make(map[string]int)
	for i := 0; i < obj.NumField(); i++ {
		field := elem.Field(i)

		fieldTag := field.Tag.Get("csv")
		if fieldTag == "" {
			// no "csv" tag set, skip this field
			continue
		}
		fieldIndexByName[fieldTag] = i
	}

	return fieldIndexByName
}

func getUnderlyingObject(rv reflect.Value) reflect.Value {
	if rv.Kind() == reflect.Pointer {
		return rv.Elem()
	}

	return rv
}
