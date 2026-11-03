// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

func TestStructuredEncoderBytes(t *testing.T) {
	v := object{{"list", array{}}, {"value", "/a<b>"}, {"nested", object{{"empty", ""}, {"count", 2}}}}
	var out bytes.Buffer
	if err := writeJSON(&out, v, serializerOptions{}); err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"list\": [\n  ],\n  \"value\": \"\\/a<b>\",\n  \"nested\": {\n    \"empty\": \"\",\n    \"count\": 2\n  }\n}\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
	out.Reset()
	if err := writeYAML(&out, v, serializerOptions{}); err != nil {
		t.Fatal(err)
	}
	want = "%YAML 1.1\n--- !!map\n!!str list: !!seq []\n!!str value: !!str /a<b>\n!!str nested: !!map\n  !!str empty: !!str\n  !!str count: !!int 2\n...\n\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
	out.Reset()
	if err := writeJSON(&out, v, serializerOptions{Compact: true}); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "{\"list\":[],\"value\":\"\\/a<b>\",\"nested\":{\"empty\":\"\",\"count\":2}}\n" {
		t.Fatal(got)
	}
}

func TestSchemaModesKeepVersionAndCommand(t *testing.T) {
	snap := testSnapshot()
	for _, version := range []string{"42", "43", "44", "45"} {
		t.Run(version, func(t *testing.T) {
			argv := []string{"/opt/crane/bin/sinfo", "--json=v0.0." + version, "-p", "compute"}
			o, err := parseOptions(argv, nil)
			if err != nil {
				t.Fatal(err)
			}
			var out, diagnostics bytes.Buffer
			err = renderStructured(&out, &diagnostics, argv, buildRows(snap, o), snap, o, func(uint32) (string, bool) { return "user", true })
			if err != nil {
				t.Fatal(err)
			}
			var data struct {
				Sinfo []map[string]any
				Meta  struct {
					Command []string
					Plugin  struct {
						Parser string `json:"data_parser"`
					}
				}
			}
			if err := json.Unmarshal(out.Bytes(), &data); err != nil {
				t.Fatal(err)
			}
			if data.Meta.Command[0] != argv[0] || data.Meta.Plugin.Parser != "data_parser/v0.0."+version || len(data.Sinfo) != 1 {
				t.Fatalf("%s", &out)
			}
			_, newReason := data.Sinfo[0]["reason_set_by_user"]
			if newReason != (version == "45") {
				t.Fatalf("wrong reason representation for %s", version)
			}
		})
	}
}

func TestPublicSchemasAreUsableDocuments(t *testing.T) {
	for version := 42; version <= 45; version++ {
		doc, err := publicSchema(version)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := writeJSON(&out, doc, serializerOptions{}); err != nil {
			t.Fatal(err)
		}
		var parsed struct {
			Type       string
			Properties map[string]any
		}
		if err := json.Unmarshal(out.Bytes(), &parsed); err != nil {
			t.Fatal(err)
		}
		if parsed.Type != "object" || parsed.Properties["sinfo"] == nil || parsed.Properties["meta"] == nil {
			t.Fatalf("invalid schema version %d", version)
		}
	}
}

func TestTimeAndDurationContracts(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 34, 56, 0, time.UTC)
	cases := []struct {
		format string
		t      time.Time
		want   string
	}{
		{"standard", now, "2026-06-04T12:34:56"},
		{"%a %e %b %Y %T %z", now, "Thu  4 Jun 2026 12:34:56 +0000"},
		{"%5d|%_5d|%05d", now, "00004|    4|00004"},
		{"%Ea|%Ob|%_6e", now, "%Ea|Jun|     4"},
		{"%#c|%^P", now, "Thu Jun  4 12:34:56 2026|pm"},
		{"%3z|%-z", now, "  +0000|+0"},
		{"relative", now.Add(-24 * time.Hour), "Ystday 12:34"},
		{"relative", now, "12:34:56"},
		{"relative", now.Add(24 * time.Hour), "Tomorr 12:34"},
		{"relative", now.Add(2 * 24 * time.Hour), "Sat 12:34"},
		{"relative", now.Add(8 * 24 * time.Hour), "12 Jun 12:34"},
		{"%F", time.Unix(0, 0), "Unknown"},
		{"%F", time.Unix(0xfffffffe, 0), "None"},
		{"%F", time.Unix(0xffffffff, 0), "Unknown"},
		{"%300Y", now, strings.Repeat("#", 255)},
	}
	for _, tc := range cases {
		if got := timeFormatter(tc.format, now, io.Discard)(tc.t); got != tc.want {
			t.Errorf("%q: %q != %q", tc.format, got, tc.want)
		}
	}
	if secondsString(60) != "1:00" || reservationDuration(60) != "00:01:00" {
		t.Fatal("partition and reservation duration contracts differ")
	}
}
