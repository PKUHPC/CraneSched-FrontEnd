// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
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
