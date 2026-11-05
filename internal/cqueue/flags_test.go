package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestFillReqJobTypes(t *testing.T) {
	original := FlagFilterJobTypes
	t.Cleanup(func() { FlagFilterJobTypes = original })

	for _, test := range []struct {
		value string
		want  []protos.JobType
	}{
		{"", nil},
		{"Interactive", []protos.JobType{protos.JobType_Interactive}},
		{"Batch", []protos.JobType{protos.JobType_Batch}},
		{"Container", []protos.JobType{protos.JobType_Container}},
		{"bAtCh,INTERACTIVE,container,Batch", []protos.JobType{protos.JobType_Interactive, protos.JobType_Batch, protos.JobType_Container}},
	} {
		t.Run(test.value, func(t *testing.T) {
			FlagFilterJobTypes = test.value
			req, err := FillReqByCobraFlags()
			if err != nil {
				t.Fatal(err)
			}
			slices.Sort(req.FilterJobTypes)
			if !slices.Equal(req.FilterJobTypes, test.want) {
				t.Fatalf("FilterJobTypes = %v, want %v", req.FilterJobTypes, test.want)
			}
		})
	}

	for _, value := range []string{"invalid", "Batch,invalid", "Batch,", ",Batch", "Batch,,Interactive"} {
		t.Run(value, func(t *testing.T) {
			FlagFilterJobTypes = value
			_, err := FillReqByCobraFlags()
			craneErr, ok := errors.AsType[*util.CraneError](err)
			if !ok || craneErr.Code != util.ErrorCmdArg || !strings.Contains(craneErr.Message, "unknown job type:") {
				t.Fatalf("want command argument error for %q, got %v", value, err)
			}
		})
	}
}
