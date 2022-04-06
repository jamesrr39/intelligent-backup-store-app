package csvx

import (
	"fmt"
	"reflect"
)

type InvalidUnmarshalError struct {
	goType reflect.Type
}

func (e InvalidUnmarshalError) Error() string {
	return fmt.Sprintf("csvx: unmarshal target must be a non-nil pointer, but got %s", e.goType)
}
