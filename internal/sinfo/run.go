// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const defaultConfigPath = "/etc/crane/config.yaml"

// Run executes the standalone command. argv includes the invocation name;
// environ contains KEY=value strings. Only main owns process exit and signals.
func Run(ctx context.Context, argv, environ []string, stdout, stderr io.Writer) int {
	env := make(map[string]string, len(environ))
	for _, entry := range environ {
		if key, value, ok := strings.Cut(entry, "="); ok {
			env[key] = value
		}
	}
	r := runner{open: func(ctx context.Context) (snapshotSource, error) {
		return openCraneSource(ctx, defaultConfigPath)
	}, now: time.Now, wait: waitForIteration, renderer: newTextRenderer()}
	return r.run(ctx, argv, env, stdout, stderr)
}

type runner struct {
	open     func(context.Context) (snapshotSource, error)
	now      func() time.Time
	wait     func(context.Context, time.Duration) error
	renderer textRenderer
}

func waitForIteration(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func reportError(w io.Writer, err error) {
	var diagnostic *commandError
	if errors.As(err, &diagnostic) {
		_, _ = io.WriteString(w, diagnostic.message)
	} else {
		_, _ = fmt.Fprintf(w, "sinfo: error: %s\n", err)
	}
}

func (r runner) run(ctx context.Context, argv []string, env map[string]string, stdout, stderr io.Writer) int {
	clock := r.now
	location := commandTimezone(env)
	r.now = func() time.Time { return clock().In(location) }
	o, err := parseOptions(argv, env)
	if err != nil {
		reportError(stderr, err)
		return 1
	}
	if o.Action != "" {
		if err := writeAction(stdout, o); err != nil {
			reportError(stderr, err)
			return 1
		}
		return 0
	}
	for _, diagnostic := range o.Diagnostics {
		if _, err := io.WriteString(stderr, diagnostic); err != nil {
			return 1
		}
	}
	if err := validateCapabilities(o); err != nil {
		reportError(stderr, err)
		return 1
	}
	// Structured output also splits groups by allocated generic resources.
	if o.Serialization != "" && !o.Reservations {
		o.Fields = append(o.Fields, formatItem{Field: fieldByName("GresUsed")})
	}
	if o.Verbose != 0 {
		if err := writeVerbose(stdout, o); err != nil {
			reportError(stderr, err)
			return 1
		}
	}
	source, err := r.open(ctx)
	if err != nil {
		reportError(stderr, err)
		return 1
	}
	defer source.Close()
	r.renderer.TimeFormat = timeFormatter(env["SLURM_TIME_FORMAT"], r.now(), stderr)
	rc := 0
	for {
		if ctx.Err() != nil {
			return rc
		}
		if !o.NoHeader && o.Serialization == "" && (o.Long || o.Iterate != 0 || o.Verbose != 0) {
			if _, err := fmt.Fprintln(stdout, r.now().Format("Mon Jan 02 15:04:05 2006")); err != nil {
				reportError(stderr, err)
				return 1
			}
		}
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		s, loadErr := source.Load(queryCtx, o)
		cancel()
		if ctx.Err() != nil {
			return rc
		}
		if loadErr != nil {
			reportError(stderr, loadErr)
			rc = 1
		} else {
			// Validate and render a complete frame before publishing it. Missing
			// controller data must not produce a half-valid table or JSON document.
			var frame bytes.Buffer
			err = r.render(&frame, stderr, argv, s, o)
			if err != nil {
				reportError(stderr, err)
				rc = 1
			} else if _, err = io.Copy(stdout, &frame); err != nil {
				reportError(stderr, err)
				return 1
			}
		}
		if o.Iterate == 0 {
			return rc
		}
		if _, err := io.WriteString(stdout, "\n"); err != nil {
			reportError(stderr, err)
			return 1
		}
		if err := r.wait(ctx, time.Duration(o.Iterate)*time.Second); err != nil {
			return rc
		}
	}
}

func (r runner) render(w, stderr io.Writer, argv []string, s *snapshot, o *options) error {
	if o.Reservations {
		return r.renderer.reservations(w, s.Reservations, o.NoHeader, r.now())
	}
	if name, single := o.NodeFilter.Single(); single {
		found := false
		for _, n := range s.Nodes {
			found = found || n.Name == name
		}
		if !found {
			return &commandError{"slurm_load_node: Invalid node name specified\n"}
		}
	}
	rows := buildRows(s, o)
	sortRows(rows, o)
	if o.Serialization != "" {
		return renderStructured(w, stderr, argv, rows, s, o, r.renderer.UserName)
	}
	return r.renderer.table(w, rows, o)
}
