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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"CraneFrontEnd/generated/protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	acceptanceOptInEnv = "CRANE_EXECUTION_FLOW_ACCEPTANCE"
	// This value deliberately violates the flow ID schema. It is never printed
	// and must not survive the trace plugin's pipeline-fault sanitization.
	rejectedCanary = "acceptance-sensitive-canary-do-not-persist"
)

var environmentIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type traceHookClient interface {
	TraceHook(context.Context, *protos.TraceHookRequest, ...grpc.CallOption) (*protos.TraceHookReply, error)
}

type submissionReceipt struct {
	OK bool `json:"ok"`
}

type options struct {
	socketPath    string
	environmentID string
	timeout       time.Duration
	jsonOutput    bool
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	flags := flag.NewFlagSet("crane-flow-fault-fixture", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var opts options
	flags.StringVar(&opts.socketPath, "socket", "", "absolute cplugind Unix socket path")
	flags.StringVar(
		&opts.environmentID,
		"environment-id",
		"",
		"expected CRANE_EXECUTION_FLOW_ENVIRONMENT_ID of the target cplugind",
	)
	flags.DurationVar(&opts.timeout, "timeout", 5*time.Second, "TraceHook RPC timeout")
	flags.BoolVar(&opts.jsonOutput, "json", false, "print a machine-readable result")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if !filepath.IsAbs(opts.socketPath) {
		return options{}, errors.New("--socket must be an absolute path")
	}
	if !environmentIDPattern.MatchString(opts.environmentID) {
		return options{}, errors.New("--environment-id is missing or invalid")
	}
	if opts.timeout <= 0 || opts.timeout > 30*time.Second {
		return options{}, errors.New("--timeout must be greater than zero and no more than 30s")
	}
	return opts, nil
}

func validateUnixSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect cplugind socket: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("cplugind socket path must not be a symbolic link")
	}
	if info.Mode()&os.ModeSocket == 0 {
		return errors.New("cplugind socket path is not a Unix socket")
	}
	return nil
}

func invalidFlowRequest(environmentID string, observedAt time.Time) *protos.TraceHookRequest {
	timestamp := timestamppb.New(observedAt.UTC())
	return &protos.TraceHookRequest{Spans: []*protos.SpanInfo{{
		TraceId:     "00000000000000000000000000000001",
		SpanId:      "0000000000000001",
		Name:        "flow/v1/ctld/job/accepted",
		StartTime:   timestamp,
		EndTime:     timestamp,
		Status:      protos.SpanStatus_SPAN_STATUS_ERROR,
		ServiceName: "cranectld",
		Attributes: map[string]string{
			"event_sequence":           "1",
			"flow_environment_id":      environmentID,
			"flow_id":                  rejectedCanary,
			"flow_schema":              "v1",
			"job_id":                   "1",
			"operation":                "submit",
			"outcome":                  "accepted",
			"point":                    "ctld/job/accepted",
			"producer":                 "cranectld",
			"service_instance":         "acceptance-fixture",
			"service_logical_instance": "cranectld",
		},
	}}}
}

func submitInvalidFlowPoint(
	ctx context.Context,
	client traceHookClient,
	environmentID string,
	observedAt time.Time,
) error {
	_, err := client.TraceHook(ctx, invalidFlowRequest(environmentID, observedAt))
	if err != nil {
		return fmt.Errorf("TraceHook RPC failed: %w", err)
	}
	return nil
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	if getenv(acceptanceOptInEnv) != "1" {
		return fmt.Errorf("%s=1 is required", acceptanceOptInEnv)
	}
	opts, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}
	if err := validateUnixSocket(opts.socketPath); err != nil {
		return err
	}

	conn, err := grpc.NewClient(
		"unix://"+opts.socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("create cplugind client: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	if err := submitInvalidFlowPoint(
		ctx,
		protos.NewCranePluginDClient(conn),
		opts.environmentID,
		time.Now(),
	); err != nil {
		return err
	}
	if opts.jsonOutput {
		return json.NewEncoder(stdout).Encode(submissionReceipt{OK: true})
	}
	_, err = fmt.Fprintf(
		stdout,
		"submitted invalid execution-flow point for environment %s\n",
		opts.environmentID,
	)
	return err
}

func main() {
	if err := run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "crane-flow-fault-fixture: %v\n", err)
		os.Exit(2)
	}
}
