package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestRequestedCPUFormatHelp(t *testing.T) {
	original := FlagFormat
	t.Cleanup(func() { FlagFormat = original })

	usage := RootCmd.Flags().Lookup("format").Usage
	match := regexp.MustCompile(`%Q/(%[A-Za-z]+)`).FindStringSubmatch(usage)
	if len(match) != 2 {
		t.Fatalf("missing long identifier for %%Q in help: %s", usage)
	}
	reply := &protos.QueryJobsInfoReply{JobInfoList: []*protos.JobInfo{{
		NodeNum:         2,
		Ntasks:          new(uint32(4)),
		ReqTotalResView: &protos.ResourceView{CpuCount: 8},
	}}}
	FlagFormat = "%Q"
	wantHeader, wantData, err := FormatData(reply)
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{match[1], strings.ToLower(match[1]), strings.ToUpper(match[1]), "%ReqCpuPerTask"} {
		t.Run(format, func(t *testing.T) {
			FlagFormat = format
			header, data, err := FormatData(reply)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(header, wantHeader) || !reflect.DeepEqual(data, wantData) {
				t.Fatalf("%s = %v %v, want %%Q output %v %v", format, header, data, wantHeader, wantData)
			}
		})
	}
}
