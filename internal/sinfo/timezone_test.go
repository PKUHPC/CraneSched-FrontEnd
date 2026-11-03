// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestCommandTimezone(t *testing.T) {
	for _, tc := range []struct{ zone, instant, want string }{
		{"", "2026-06-04T12:34:56Z", "2026-06-04T12:34:56 +0000 UTC"},
		{"UTC-8", "2026-06-04T12:34:56Z", "2026-06-04T20:34:56 +0800 UTC"},
		{"CST-8", "2026-06-04T12:34:56Z", "2026-06-04T20:34:56 +0800 CST"},
		{"<+0530>-5:30", "2026-06-04T12:34:56Z", "2026-06-04T18:04:56 +0530 +0530"},
		{"Asia/Shanghai", "2026-06-04T12:34:56Z", "2026-06-04T20:34:56 +0800 CST"},
		{":/usr/share/zoneinfo/Asia/Shanghai", "2026-06-04T12:34:56Z", "2026-06-04T20:34:56 +0800 CST"},
		{"EST5EDT,M3.2.0,M11.1.0", "2026-03-08T06:59:59Z", "2026-03-08T01:59:59 -0500 EST"},
		{"EST5EDT,M3.2.0,M11.1.0", "2026-03-08T07:00:00Z", "2026-03-08T03:00:00 -0400 EDT"},
		{"EST5EDT,M3.2.0,M11.1.0", "2026-11-01T05:59:59Z", "2026-11-01T01:59:59 -0400 EDT"},
		{"EST5EDT,M3.2.0,M11.1.0", "2026-11-01T06:00:00Z", "2026-11-01T01:00:00 -0500 EST"},
		{"AEST-10AEDT,M10.1.0,M4.1.0/3", "2026-01-01T00:00:00Z", "2026-01-01T11:00:00 +1100 AEDT"},
		{"AEST-10AEDT,M10.1.0,M4.1.0/3", "2026-07-01T00:00:00Z", "2026-07-01T10:00:00 +1000 AEST"},
	} {
		t.Run(tc.zone+tc.instant, func(t *testing.T) {
			instant, err := time.Parse(time.RFC3339, tc.instant)
			if err != nil {
				t.Fatal(err)
			}
			got := instant.In(commandTimezone(map[string]string{"TZ": tc.zone})).Format("2006-01-02T15:04:05 -0700 MST")
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunUsesTZForHeadingAndReason(t *testing.T) {
	s := testSnapshot()
	s.Nodes[0].ReasonTime = time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	source := &memorySource{load: func(context.Context, *options) (*snapshot, error) { return s, nil }}
	r := testRunner(source)
	var out, diagnostic bytes.Buffer
	code := r.run(context.Background(), []string{"sinfo", "-v", "-n", "n01", "-o", "%H"}, map[string]string{"TZ": "UTC-8"}, &out, &diagnostic)
	if code != 0 || !strings.Contains(out.String(), "Thu Jun 04 20:34:56 2026\n") || !strings.Contains(out.String(), "2026-06-04T20:00:00") {
		t.Fatalf("code=%d out=%q err=%q", code, &out, &diagnostic)
	}
}

func TestReservationTextEndSecondRemainsActive(t *testing.T) {
	start := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	res := []reservation{{Name: "maint", Start: start, End: start.Add(time.Minute)}}
	for _, offset := range []time.Duration{0, 500 * time.Millisecond, time.Second} {
		var out bytes.Buffer
		if err := newTextRenderer().reservations(&out, res, true, res[0].End.Add(offset)); err != nil {
			t.Fatal(err)
		}
		inactive := strings.Contains(out.String(), "INACTIVE")
		if inactive != (offset >= time.Second) {
			t.Fatalf("offset=%s out=%q", offset, &out)
		}
	}
}
