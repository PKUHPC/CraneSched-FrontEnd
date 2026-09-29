package ccon

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type privacyQueryClient struct {
	protos.CraneCtldClient
	reply *protos.QueryJobsInfoReply
}

func (c privacyQueryClient) QueryJobsInfo(_ context.Context, request *protos.QueryJobsInfoRequest, _ ...grpc.CallOption) (*protos.QueryJobsInfoReply, error) {
	if request.Uid == nil {
		panic("missing caller UID")
	}
	return c.reply, nil
}

func usePublicContainer(t *testing.T) {
	t.Helper()
	oldStub, oldFlags := stub, flags
	t.Cleanup(func() { stub, flags = oldStub, oldFlags })
	flags = &Flags{}
	public := protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC
	stub = privacyQueryClient{reply: &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{{
		JobId: 42, Type: protos.JobType_Container, QueryVisibility: public, Partition: "cpu",
		StepInfoList: []*protos.StepInfo{{JobId: 42, StepId: 1, Type: protos.JobType_Container, Status: protos.JobStatus_Running, QueryVisibility: public}},
	}}}}
}

func capturePrivacyOutput(t *testing.T, run func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old; r.Close(); w.Close() }()
	result := make(chan string, 1)
	go func() { data, _ := io.ReadAll(r); result <- string(data) }()
	err = run()
	w.Close()
	return <-result, err
}

func TestRestrictedContainerDetailsFailBeforeMetadataOrFileAccess(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			run  func(*cobra.Command, []string) error
			args []string
		}{
			{"logs", logExecute, []string{"42.1"}},
			{"inspect", inspectStepExecute, []string{"42.1"}},
			{"inspectp", inspectPodExecute, []string{"42"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				usePublicContainer(t)
				flags.Global.Json = asJSON
				output, err := capturePrivacyOutput(t, func() error { return tc.run(&cobra.Command{}, tc.args) })
				var craneErr *util.CraneError
				if !errors.As(err, &craneErr) || craneErr.Code == 0 || !strings.Contains(err.Error(), "restricted") {
					t.Fatalf("expected restricted nonzero error, got %v", err)
				}
				if output != "" {
					t.Fatalf("unexpected output: %q", output)
				}
			})
		}
	}
}

func TestWaitDoesNotTreatRestrictedContainerAsFinished(t *testing.T) {
	usePublicContainer(t)
	done, result, err := checkContainerStepsDone(42)
	if err != nil || done || len(result.Steps) != 1 {
		t.Fatalf("done=%v result=%v err=%v", done, result, err)
	}
}

func TestPublicContainerSummariesRemainVisible(t *testing.T) {
	usePublicContainer(t)
	text, err := capturePrivacyOutput(t, func() error { return psExecute(&cobra.Command{}, nil) })
	if err != nil || !strings.Contains(text, "42.1") || strings.Count(text, util.RestrictedJobField) != 2 {
		t.Fatalf("text=%q err=%v", text, err)
	}
	text, err = capturePrivacyOutput(t, func() error { return podExecute(&cobra.Command{}, nil) })
	if err != nil || strings.Count(text, util.RestrictedJobField) != 2 {
		t.Fatalf("text=%q err=%v", text, err)
	}
	flags.Global.Json = true
	text, err = capturePrivacyOutput(t, func() error { return psExecute(&cobra.Command{}, nil) })
	if err != nil {
		t.Fatal(err)
	}
	var data struct {
		Data []struct {
			JobId           uint32                 `json:"job_id"`
			Image           string                 `json:"image"`
			Command         string                 `json:"command"`
			QueryVisibility protos.QueryVisibility `json:"query_visibility"`
		} `json:"data"`
	}
	if err = json.Unmarshal([]byte(text), &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Data) != 1 || data.Data[0].JobId != 42 || data.Data[0].Image != "" ||
		data.Data[0].Command != "" || data.Data[0].QueryVisibility != protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC ||
		strings.Contains(text, util.RestrictedJobField) {
		t.Fatalf("summary missing or text placeholder in JSON: %s", text)
	}
}
