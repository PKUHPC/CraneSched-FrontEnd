package util

import (
	"reflect"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestParseHostListExpandsNodeNameExpression(t *testing.T) {
	got, ok := ParseHostList("b1u01n1,b2u[05,02]n3,b3u03n4")
	if !ok {
		t.Fatal("ParseHostList rejected a valid hostlist expression")
	}

	want := []string{"b1u01n1", "b2u05n3", "b2u02n3", "b3u03n4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseHostList returned %v, want %v", got, want)
	}
}

func TestExpandHostListSharedSuffix(t *testing.T) {
	got, err := ExpandHostList("b1u01n1,b2u[05,02]n3,b3u03n4")
	if err != nil {
		t.Fatalf("ExpandHostList returned error: %v", err)
	}

	want := []string{"b1u01n1", "b2u05n3", "b2u02n3", "b3u03n4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExpandHostList returned %v, want %v", got, want)
	}
}

func TestCheckJobArgsNormalizesNodeLists(t *testing.T) {
	job := &protos.JobToCtld{
		NodeNumMin: 1,
		NodeNumMax: 1,
		Ntasks:     1,
		TimeLimit:  durationpb.New(time.Second),
		Nodelist:   "b2u[05,02]n3",
		Excludes:   "b3u[03-04]n4",
	}

	if err := CheckJobArgs(job); err != nil {
		t.Fatalf("CheckJobArgs returned error: %v", err)
	}
	if job.Nodelist != "b2u05n3,b2u02n3" {
		t.Fatalf("Nodelist = %q, want %q", job.Nodelist, "b2u05n3,b2u02n3")
	}
	if job.Excludes != "b3u03n4,b3u04n4" {
		t.Fatalf("Excludes = %q, want %q", job.Excludes, "b3u03n4,b3u04n4")
	}
}

func TestCheckStepArgsNormalizesNodeLists(t *testing.T) {
	step := &protos.StepToCtld{
		NodeNum:   1,
		Ntasks:    1,
		TimeLimit: durationpb.New(time.Second),
		Nodelist:  "b2u[05,02]n3",
		Excludes:  "b3u[03-04]n4",
	}

	if err := CheckStepArgs(step); err != nil {
		t.Fatalf("CheckStepArgs returned error: %v", err)
	}
	if step.Nodelist != "b2u05n3,b2u02n3" {
		t.Fatalf("Nodelist = %q, want %q", step.Nodelist, "b2u05n3,b2u02n3")
	}
	if step.Excludes != "b3u03n4,b3u04n4" {
		t.Fatalf("Excludes = %q, want %q", step.Excludes, "b3u03n4,b3u04n4")
	}
}

func TestNormalizeNodeList(t *testing.T) {
	got, err := NormalizeNodeList("node[01-02],node04")
	if err != nil {
		t.Fatalf("NormalizeNodeList returned error: %v", err)
	}

	want := "node01,node02,node04"
	if got != want {
		t.Fatalf("NormalizeNodeList returned %q, want %q", got, want)
	}
}

func TestNormalizeNodeListRejectsMalformedExpression(t *testing.T) {
	if _, err := NormalizeNodeList("node[01,foo]"); err == nil {
		t.Fatal("NormalizeNodeList accepted malformed expression")
	}
	if _, err := NormalizeNodeList("[]"); err == nil {
		t.Fatal("NormalizeNodeList accepted empty expression")
	}
}
