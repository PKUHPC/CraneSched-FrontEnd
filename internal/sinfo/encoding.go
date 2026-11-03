// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"

	"gopkg.in/yaml.v3"
)

func jsonScalar(v any) string {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	_ = e.Encode(v)
	// json-c escapes forward slashes, including argv[0] and plugin names.
	return strings.ReplaceAll(strings.TrimSuffix(b.String(), "\n"), "/", `\/`)
}

func writeJSON(w io.Writer, root any, options serializerOptions) error {
	var out strings.Builder
	var emit func(any, int)
	indent := func(depth int) { out.WriteString(strings.Repeat("  ", depth)) }
	newline := func(depth int) {
		if !options.Compact {
			out.WriteByte('\n')
			indent(depth)
		}
	}
	emit = func(value any, depth int) {
		switch v := value.(type) {
		case object:
			out.WriteByte('{')
			for i, m := range v {
				if i > 0 {
					out.WriteByte(',')
				}
				newline(depth + 1)
				out.WriteString(jsonScalar(m.name))
				out.WriteByte(':')
				if !options.Compact {
					out.WriteByte(' ')
				}
				emit(m.value, depth+1)
			}
			if len(v) > 0 {
				newline(depth)
			}
			out.WriteByte('}')
		case array:
			out.WriteByte('[')
			for i, child := range v {
				if i > 0 {
					out.WriteByte(',')
				}
				newline(depth + 1)
				emit(child, depth+1)
			}
			newline(depth)
			out.WriteByte(']')
		case float64:
			switch {
			case math.IsInf(v, 0):
				if options.Complex {
					out.WriteString("Infinity")
				} else {
					out.WriteString("1.8446744073709552e+19")
				}
			case math.IsNaN(v):
				if options.Complex {
					out.WriteString("NaN")
				} else {
					out.WriteString("1.8446744073709552e+19")
				}
			default:
				out.WriteString(jsonScalar(v))
			}
		default:
			out.WriteString(jsonScalar(v))
		}
	}
	emit(root, 0)
	out.WriteByte('\n')
	_, err := io.WriteString(w, out.String())
	return err
}

func yamlScalar(value any, noTag bool) (string, error) {
	tag, text := "!!str", ""
	switch v := value.(type) {
	case nil:
		tag, text = "!!null", "null"
	case string:
		text = v
	case bool:
		tag, text = "!!bool", fmt.Sprint(v)
	case float64:
		tag, text = "!!float", fmt.Sprintf("%f", v)
	case json.Number:
		text, tag = v.String(), "!!int"
		if strings.ContainsAny(text, ".eE") {
			f, err := v.Float64()
			if err != nil {
				return "", err
			}
			tag, text = "!!float", fmt.Sprintf("%f", f)
		}
	default:
		tag, text = "!!int", fmt.Sprint(v)
	}
	if text == "" {
		if noTag {
			return "", nil
		}
		return tag, nil
	}
	node := yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: text, Style: yaml.TaggedStyle}
	// libyaml chooses a quoted scalar for embedded line breaks. yaml.v3's
	// automatic literal-block style would change the command's byte contract.
	if tag == "!!str" && strings.Contains(text, "\n") {
		node.Style |= yaml.SingleQuotedStyle
	}
	data, err := yaml.Marshal(&node)
	if err != nil {
		return "", err
	}
	s := strings.TrimSuffix(string(data), "\n")
	s = strings.TrimSuffix(s, "\n...")
	if noTag {
		s = strings.TrimPrefix(s, tag+" ")
	}
	return s, nil
}

func writeYAML(w io.Writer, root object, options serializerOptions) error {
	var out strings.Builder
	out.WriteString("%YAML 1.1\n---")
	if !options.NoTag {
		out.WriteString(" !!map")
	}
	out.WriteByte('\n')
	var mapping func(object, int, string) error
	var sequence func(array, int) error
	var value func(any, int) error
	space := func(depth int) { out.WriteString(strings.Repeat(" ", depth)) }
	value = func(v any, depth int) error {
		switch v := v.(type) {
		case object:
			if !options.NoTag {
				out.WriteString(" !!map")
			}
			if len(v) == 0 {
				out.WriteString(" {}\n")
				return nil
			}
			out.WriteByte('\n')
			return mapping(v, depth+2, "")
		case array:
			if !options.NoTag {
				out.WriteString(" !!seq")
			}
			if len(v) == 0 {
				out.WriteString(" []\n")
				return nil
			}
			out.WriteByte('\n')
			return sequence(v, depth)
		default:
			text, err := yamlScalar(v, options.NoTag)
			if err != nil {
				return err
			}
			if text != "" {
				out.WriteByte(' ')
				out.WriteString(strings.ReplaceAll(text, "\n", "\n"+strings.Repeat(" ", depth+2)))
			}
			out.WriteByte('\n')
			return nil
		}
	}
	mapping = func(v object, depth int, firstPrefix string) error {
		for i, m := range v {
			if i == 0 && firstPrefix != "" {
				space(depth - 2)
				out.WriteString(firstPrefix)
			} else {
				space(depth)
			}
			key, err := yamlScalar(m.name, options.NoTag)
			if err != nil {
				return err
			}
			out.WriteString(key)
			out.WriteByte(':')
			if err := value(m.value, depth); err != nil {
				return err
			}
		}
		return nil
	}
	sequence = func(v array, depth int) error {
		for _, child := range v {
			if obj, ok := child.(object); ok && options.NoTag && len(obj) > 0 {
				if err := mapping(obj, depth+2, "- "); err != nil {
					return err
				}
				continue
			}
			space(depth)
			out.WriteByte('-')
			if err := value(child, depth); err != nil {
				return err
			}
		}
		return nil
	}
	if err := mapping(root, 0, ""); err != nil {
		return err
	}
	out.WriteString("...\n\n")
	_, err := io.WriteString(w, out.String())
	return err
}
