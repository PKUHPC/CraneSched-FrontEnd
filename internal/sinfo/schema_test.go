// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestSchemaV45Contract(t *testing.T) {
	var out bytes.Buffer
	if err := writeJSON(&out, publicSchema(), serializerOptions{Compact: true}); err != nil {
		t.Fatal(err)
	}
	// Slurm 26.05.4 v45 spec_only, compacted in property order, excluding the
	// five removed partition fields and their now-empty oversubscribe container.
	// This fingerprint covers descriptions, enums, formats and empty required
	// arrays without embedding a second schema definition in the executable.
	const want = "07c046c68c5c48a4670a6785e1250149ba94867b832b243fb099b09c5d89e3e8"
	if got := fmt.Sprintf("%x", sha256.Sum256(out.Bytes())); got != want {
		t.Fatalf("v45 schema contract changed: %s; compare against the reference before updating the fingerprint", got)
	}
}

func TestStructuredParserScope(t *testing.T) {
	for _, kind := range []string{"json", "yaml"} {
		for _, parser := range []string{"list", "v0.0.41", "v0.0.42+spec_only", "data_parser/v0.0.43", "v0.0.44", "v0.0.46", "", "notreal"} {
			t.Run(kind+"/"+parser, func(t *testing.T) {
				source := &memorySource{load: func(context.Context, *options) (*snapshot, error) { return testSnapshot(), nil }}
				var out, diagnostic bytes.Buffer
				code := testRunner(source).run(context.Background(), []string{"sinfo", "--" + kind + "=" + parser}, nil, &out, &diagnostic)
				if parser == "list" {
					if code != 0 || out.String() != "data_parser/v0.0.45\n" || diagnostic.String() != "Possible data_parser plugins:\n" {
						t.Fatalf("%d %q %q", code, &out, &diagnostic)
					}
				} else if code != 1 || out.Len() != 0 || !strings.Contains(diagnostic.String(), "supported: v0.0.45") {
					t.Fatalf("unsupported parser fell back: %d %q %q", code, &out, &diagnostic)
				}
			})
		}
	}
}

func TestStructuredV45Report(t *testing.T) {
	for _, parser := range []string{"", "=v0.0.45", "=data_parser/v0.0.45", "=v0.0.45+complex"} {
		t.Run(parser, func(t *testing.T) {
			argv := []string{"/opt/crane/bin/sinfo", "--json" + parser, "-p", "compute"}
			o, err := parseOptions(argv, nil)
			if err != nil {
				t.Fatal(err)
			}
			snap := testSnapshot()
			// Values above 32 bits must not be truncated by schema format annotations.
			snap.Nodes[0].CPUs = 1 << 33
			snap.Partitions[0].DefaultMemCPU = 1024
			var out, diagnostics bytes.Buffer
			if err := renderStructured(&out, &diagnostics, argv, buildRows(snap, o), snap, o, func(uint32) (string, bool) { return "user", true }); err != nil {
				t.Fatal(err)
			}
			var data map[string]any
			dec := json.NewDecoder(&out)
			dec.UseNumber()
			if err := dec.Decode(&data); err != nil {
				t.Fatal(err)
			}
			meta := data["meta"].(map[string]any)
			command := meta["command"].([]any)
			if command[0] != argv[0] || !strings.HasPrefix(meta["plugin"].(map[string]any)["data_parser"].(string), "data_parser/v0.0.45") {
				t.Fatal(meta)
			}
			row := data["sinfo"].([]any)[0].(map[string]any)
			if row["cpus"].(map[string]any)["maximum"] != json.Number("8589934592") {
				t.Fatal(row["cpus"])
			}
			if _, ok := row["reason"].(map[string]any)["user"]; ok {
				t.Fatal("legacy reason.user emitted")
			}
			if row["reason_set_by_user"] != "" {
				t.Fatal("empty reason user must stay a string")
			}
			partition := row["partition"].(map[string]any)
			defaults, maximums := partition["defaults"].(map[string]any), partition["maximums"].(map[string]any)
			if _, ok := defaults["memory_per_cpu"]; ok {
				t.Fatal("removed default memory emitted")
			}
			for _, key := range []string{"memory_per_cpu", "shares", "oversubscribe"} {
				if _, ok := maximums[key]; ok {
					t.Fatalf("removed maximum %s emitted", key)
				}
			}
			if _, ok := defaults["partition_memory_per_node"]; !ok {
				t.Fatal("active memory field missing")
			}
			if !strings.Contains(parser, "complex") {
				var schema map[string]any
				var buf bytes.Buffer
				if err := writeJSON(&buf, publicSchema(), serializerOptions{}); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(buf.Bytes(), &schema); err != nil {
					t.Fatal(err)
				}
				checkReportSchema(t, schema, data, "root")
			} else if defaults["partition_memory_per_cpu"] != json.Number("1024") || defaults["partition_memory_per_node"] != nil {
				t.Fatal(defaults)
			}
		})
	}
}

// Check every emitted field against the public schema, including fields nested
// in arrays. This catches drift between the report adapter and schema traversal.
func checkReportSchema(t *testing.T, schema map[string]any, value any, path string) {
	t.Helper()
	switch schema["type"] {
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("%s: expected object, got %T", path, value)
		}
		props := schema["properties"].(map[string]any)
		if len(obj) != len(props) {
			t.Fatalf("%s: property count %d != %d", path, len(obj), len(props))
		}
		for k, v := range props {
			child, ok := obj[k]
			if !ok {
				t.Fatalf("%s.%s missing", path, k)
			}
			checkReportSchema(t, v.(map[string]any), child, path+"."+k)
		}
	case "array":
		a, ok := value.([]any)
		if !ok {
			t.Fatalf("%s: expected array, got %T", path, value)
		}
		for _, v := range a {
			checkReportSchema(t, schema["items"].(map[string]any), v, path+"[]")
		}
	case "string":
		if _, ok := value.(string); !ok {
			t.Fatalf("%s: expected string, got %T", path, value)
		}
	case "integer":
		if _, ok := value.(json.Number); !ok {
			t.Fatalf("%s: expected number, got %T", path, value)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			t.Fatalf("%s: expected bool, got %T", path, value)
		}
	default:
		t.Fatalf("%s: unknown schema type %v", path, schema["type"])
	}
}

func TestReportNumericModes(t *testing.T) {
	for _, tc := range []struct {
		name            string
		value           uint64
		unset, infinite bool
		plain, complex  string
	}{
		{"zero", 0, false, false, `{"set":true,"infinite":false,"number":0}`, `0`},
		{"wide", 1<<63 + 1, false, false, `{"set":true,"infinite":false,"number":9223372036854775809}`, `9223372036854775809`},
		{"unset", 123, true, false, `{"set":false,"infinite":false,"number":0}`, `null`},
		{"infinite", 123, false, true, `{"set":false,"infinite":true,"number":0}`, `"Infinity"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, complex := range []bool{false, true} {
				var out bytes.Buffer
				number := reportNumeric[uint64](tc.value, tc.unset, tc.infinite)
				if err := writeJSON(&out, reportValue(reflect.ValueOf(number), complex, false), serializerOptions{Compact: true}); err != nil {
					t.Fatal(err)
				}
				want := tc.plain
				if complex {
					want = tc.complex
				}
				if out.String() != want+"\n" {
					t.Fatalf("%q != %q", &out, want)
				}
			}
		})
	}
}
