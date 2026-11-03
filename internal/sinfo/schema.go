// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"fmt"
	"reflect"
	"strings"
)

func publicSchema() object {
	return reportSchema(reflect.TypeFor[reportResponse](), "", "")
}

// Walk only our report types. Unlike a general OpenAPI generator, this preserves
// declaration order, inline types and explicit empty required arrays used by
// the CLI's spec_only response. The same types also carry the actual data.
func reportSchema(t reflect.Type, description, enum string) object {
	var kind, format string
	switch t.Kind() {
	case reflect.Struct:
		kind = "object"
	case reflect.Slice:
		kind = "array"
	case reflect.String:
		kind = "string"
	case reflect.Bool:
		kind = "boolean"
	case reflect.Uint64, reflect.Int64:
		kind, format = "integer", "int64"
		if t == reflect.TypeFor[reportInt32]() {
			format = "int32"
		}
	default:
		panic(fmt.Sprintf("unsupported sinfo report type %s", t))
	}
	result := object{{"type", kind}}
	if format != "" {
		result = append(result, member{"format", format})
	}
	if description != "" {
		result = append(result, member{"description", description})
	}
	switch t.Kind() {
	case reflect.Struct:
		properties := object{}
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			properties = append(properties, member{field.Tag.Get("json"), reportSchema(field.Type, field.Tag.Get("description"), field.Tag.Get("enum"))})
		}
		result = append(result, member{"properties", properties})
		if required, ok := reflect.Zero(t).Interface().(interface{ schemaRequired() []string }); ok {
			result = append(result, member{"required", stringsArray(required.schemaRequired())})
		}
	case reflect.Slice:
		items := reportSchema(t.Elem(), "", "")
		if enum != "" {
			items = append(object{{"enum", stringsArray(strings.Split(enum, "|"))}}, items...)
		}
		result = append(result, member{"items", items})
	}
	return result
}

// Preserve the report's field order for both encoders. Complex mode changes
// optional scalar values, but leaves string array elements (including argv)
// intact. Numeric wrappers own their unset/infinite representation.
func reportValue(v reflect.Value, complex, keepEmpty bool) any {
	if complex {
		if number, ok := v.Interface().(interface{ complexValue() any }); ok {
			return number.complexValue()
		}
	}
	switch v.Kind() {
	case reflect.Struct:
		result := object{}
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			result = append(result, member{field.Tag.Get("json"), reportValue(v.Field(i), complex, field.Tag.Get("complex") == "keep_empty")})
		}
		return result
	case reflect.Slice:
		result := make(array, v.Len())
		for i := range result {
			result[i] = reportValue(v.Index(i), complex, true)
		}
		return result
	case reflect.String:
		if complex && !keepEmpty && v.String() == "" {
			return nil
		}
		return v.String()
	case reflect.Bool:
		return v.Bool()
	case reflect.Uint64:
		return v.Uint()
	case reflect.Int64:
		return v.Int()
	default:
		panic(fmt.Sprintf("unsupported sinfo report type %s", v.Type()))
	}
}
