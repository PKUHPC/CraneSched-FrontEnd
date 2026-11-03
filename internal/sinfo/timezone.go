// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// commandTimezone supports both zoneinfo names/files and POSIX TZ expressions.
// Go's time.Local only supports the former. Supplying the expression as a TZif
// v2 footer lets the standard library evaluate offsets and DST transitions.
func commandTimezone(env map[string]string) *time.Location {
	name, set := env["TZ"]
	if !set {
		return time.Local
	}
	name = strings.TrimPrefix(name, ":")
	if name == "" {
		return time.UTC
	}
	path := name
	if !filepath.IsAbs(path) {
		dir, ok := env["TZDIR"]
		if !ok {
			dir = "/usr/share/zoneinfo"
		}
		path = filepath.Join(dir, name)
	}
	if data, err := os.ReadFile(path); err == nil {
		if location, err := time.LoadLocationFromTZData(name, data); err == nil {
			return location
		}
	}
	// Each TZif section contains one UTC type and no transitions; the footer
	// supplies the rule for all instants. The UTC type is also the fallback for
	// malformed expressions. The two sections have the same size without times.
	section := make([]byte, 54)
	copy(section, "TZif2")
	section[39] = 1 // typecnt
	section[43] = 4 // charcnt
	copy(section[50:], "UTC\x00")
	data := append(append([]byte{}, section...), section...)
	data = append(data, ('\n'))
	data = append(data, name...)
	data = append(data, '\n')
	location, err := time.LoadLocationFromTZData(name, data)
	if err != nil {
		return time.UTC
	}
	return location
}
