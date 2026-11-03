// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
)

// These are the public schema documents reported by sinfo 26.05.4's
// +SPEC_ONLY interface, not imported Slurm implementation code. Descriptions
// and property ordering are observable command output, like --help text.
//
//go:embed schema/*.json
var publicSchemas embed.FS

func publicSchema(version int) (object, error) {
	data, err := publicSchemas.ReadFile(fmt.Sprintf("schema/v%d.json", version))
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var read func() (any, error)
	read = func() (any, error) {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		switch token {
		case json.Delim('{'):
			result := object{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				value, err := read()
				if err != nil {
					return nil, err
				}
				result = append(result, member{key.(string), value})
			}
			_, err = decoder.Token()
			return result, err
		case json.Delim('['):
			result := array{}
			for decoder.More() {
				value, err := read()
				if err != nil {
					return nil, err
				}
				result = append(result, value)
			}
			_, err = decoder.Token()
			return result, err
		default:
			return token, nil
		}
	}
	root, err := read()
	if err != nil {
		return nil, err
	}
	return root.(object), nil
}
