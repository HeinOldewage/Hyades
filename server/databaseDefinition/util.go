package databaseDefinition

import (
	"reflect"
)

func LoadInto(recipient, from interface{}) {

	recipientval := reflect.Indirect(reflect.ValueOf(recipient))
	recipienttyp := recipientval.Type()
	fromval := reflect.Indirect(reflect.ValueOf(from))

	for k := 0; k < recipienttyp.NumField(); k++ {
		valtocopy := fromval.FieldByName(recipienttyp.Field(k).Name)
		if valtocopy == (reflect.Value{}) {
			//field not found in from
		} else {
			if recipientval.Field(k).Type() == valtocopy.Type() {

				recipientval.Field(k).Set(valtocopy)
			}
		}

	}
}
