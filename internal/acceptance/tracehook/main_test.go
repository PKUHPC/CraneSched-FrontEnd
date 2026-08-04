//go:build execution_flow_acceptance

/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

type recordingPluginDaemon struct {
	protos.UnimplementedCranePluginDServer

	mu      sync.Mutex
	request *protos.TraceHookRequest
}

func (d *recordingPluginDaemon) TraceHook(
	_ context.Context,
	request *protos.TraceHookRequest,
) (*protos.TraceHookReply, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.request = request
	return &protos.TraceHookReply{}, nil
}

func (d *recordingPluginDaemon) capturedRequest() *protos.TraceHookRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.request
}

func serveRecordingPluginDaemon(t *testing.T) (string, *recordingPluginDaemon) {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "cplugind.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	daemon := &recordingPluginDaemon{}
	protos.RegisterCranePluginDServer(server, daemon)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return socketPath, daemon
}

func TestRunSubmitsInvalidPointThroughTraceHookWithoutPrintingCanary(t *testing.T) {
	socketPath, daemon := serveRecordingPluginDaemon(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := run(
		[]string{
			"--socket", socketPath,
			"--environment-id", "run-1.shard-0",
			"--json",
		},
		func(key string) string {
			if key == acceptanceOptInEnv {
				return "1"
			}
			return ""
		},
		&stdout,
		&stderr,
	)
	if err != nil {
		t.Fatalf("run() error = %v, stderr=%s", err, stderr.String())
	}

	request := daemon.capturedRequest()
	if request == nil || len(request.GetSpans()) != 1 {
		t.Fatalf("captured request = %#v", request)
	}
	span := request.GetSpans()[0]
	if span.GetName() != "flow/v1/ctld/job/accepted" ||
		span.GetAttributes()["flow_id"] != rejectedCanary {
		t.Fatalf("captured invalid flow span = %#v", span)
	}
	if _, supplied := span.GetAttributes()["flow_environment_id"]; supplied {
		t.Fatal("acceptance fixture supplied frontend-owned flow_environment_id on the wire")
	}
	if span.GetEndTime() == nil || span.GetEndTime().CheckValid() != nil {
		t.Fatalf("captured event time = %#v", span.GetEndTime())
	}

	var receipt submissionReceipt
	if err := json.Unmarshal(stdout.Bytes(), &receipt); err != nil {
		t.Fatalf("decode receipt: %v", err)
	}
	if !receipt.OK {
		t.Fatalf("receipt = %#v", receipt)
	}
	if got := stdout.String(); got != "{\"ok\":true}\n" {
		t.Fatalf("JSON output = %q", got)
	}
	if strings.Contains(stdout.String(), rejectedCanary) ||
		strings.Contains(stderr.String(), rejectedCanary) {
		t.Fatal("acceptance output leaked the rejected canary")
	}
}

func TestInvalidFlowRequestContainsCanaryOnlyInRejectedFlowID(t *testing.T) {
	request := invalidFlowRequest(time.Unix(1, 2))
	encoded, err := protojson.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(string(encoded), rejectedCanary); count != 1 {
		t.Fatalf("rejected canary count = %d, request=%s", count, encoded)
	}
	span := request.GetSpans()[0]
	if _, supplied := span.GetAttributes()["flow_environment_id"]; supplied {
		t.Fatal("frontend-owned flow environment leaked into producer wire attributes")
	}
	if span.GetAttributes()["flow_schema"] != "v1" ||
		span.GetAttributes()["point"] != "ctld/job/accepted" {
		t.Fatalf("schema envelope = %#v", span.GetAttributes())
	}
}

func TestRunRequiresExplicitAcceptanceOptIn(t *testing.T) {
	var output bytes.Buffer
	err := run(nil, func(string) string { return "" }, &output, &output)
	if err == nil || !strings.Contains(err.Error(), acceptanceOptInEnv+"=1") {
		t.Fatalf("run() error = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("unexpected output = %q", output.String())
	}
}

func TestRunHumanOutputDoesNotContainRejectedCanary(t *testing.T) {
	socketPath, _ := serveRecordingPluginDaemon(t)
	var output bytes.Buffer
	err := run(
		[]string{"--socket", socketPath, "--environment-id", "run-3.shard-2"},
		func(string) string { return "1" },
		&output,
		&output,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "run-3.shard-2") ||
		strings.Contains(output.String(), rejectedCanary) {
		t.Fatalf("human output = %q", output.String())
	}
}

func TestParseOptionsRejectsNonUnixTargetsAndInvalidEnvironment(t *testing.T) {
	regularFile := filepath.Join(t.TempDir(), "not-a-socket")
	if err := os.WriteFile(regularFile, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateUnixSocket(regularFile); err == nil ||
		!strings.Contains(err.Error(), "not a Unix socket") {
		t.Fatalf("validateUnixSocket() error = %v", err)
	}
	symlink := regularFile + ".link"
	if err := os.Symlink(regularFile, symlink); err != nil {
		t.Fatal(err)
	}
	if err := validateUnixSocket(symlink); err == nil ||
		!strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("validateUnixSocket(symlink) error = %v", err)
	}

	for _, args := range [][]string{
		{"--socket", "127.0.0.1:10012", "--environment-id", "run-1"},
		{"--socket", "/tmp/cplugind.sock", "--environment-id", "secret/value"},
		{"--socket", "/tmp/cplugind.sock", "--environment-id", "run-1", "--timeout", "31s"},
	} {
		if _, err := parseOptions(args, ioDiscard{}); err == nil {
			t.Fatalf("parseOptions(%v) succeeded", args)
		}
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(value []byte) (int, error) { return len(value), nil }
