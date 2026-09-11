package cacct

import (
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func accountingTimestamp(seconds int64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(seconds, 0).UTC())
}

func accountingJob(status protos.JobStatus, start, end int64,
	elapsed *durationpb.Duration) *JobOrStep {
	return &JobOrStep{job: &protos.JobInfo{
		Status:      status,
		StartTime:   accountingTimestamp(start),
		EndTime:     accountingTimestamp(end),
		ElapsedTime: elapsed,
	}, isStep: false}
}

func accountingStep(status protos.JobStatus, start, end int64,
	elapsed *durationpb.Duration) *JobOrStep {
	return &JobOrStep{stepInfo: &protos.StepInfo{
		Status:      status,
		StartTime:   accountingTimestamp(start),
		EndTime:     accountingTimestamp(end),
		ElapsedTime: elapsed,
	}, isStep: true}
}

func TestProcessAccountingTerminalElapsed(t *testing.T) {
	const start = int64(1_700_000_000)
	statuses := []protos.JobStatus{
		protos.JobStatus_Completed,
		protos.JobStatus_Failed,
		protos.JobStatus_Cancelled,
		protos.JobStatus_ExceedTimeLimit,
		protos.JobStatus_OutOfMemory,
		protos.JobStatus_Deadline,
	}
	for _, status := range statuses {
		if got, ok := validAccountingTime(accountingTimestamp(start)); !ok {
			t.Fatalf("timestamp invalid: %v", got)
		}
		for _, item := range []*JobOrStep{
			accountingJob(status, start, start+5, nil),
			accountingStep(status, start, start+5, nil),
		} {
			if got := ProcessElapsedTime(item); got != "00:00:05" {
				t.Errorf("status %v elapsed = %q, want 00:00:05", status, got)
			}
			if got := ProcessEndTime(item); got == "unknown" {
				t.Errorf("status %v end time was unknown", status)
			}
		}
	}
}

func TestProcessAccountingZeroElapsedAndBackendPreference(t *testing.T) {
	const start = int64(1_700_000_000)
	item := accountingJob(protos.JobStatus_Cancelled, start, start,
		durationpb.New(0))
	if got := ProcessElapsedTime(item); got != "00:00:00" {
		t.Fatalf("zero backend elapsed = %q, want 00:00:00", got)
	}
	if got := ProcessEndTime(item); got == "unknown" {
		t.Fatal("equal start/end must still display an end time")
	}
	step := accountingStep(protos.JobStatus_Cancelled, start, start,
		durationpb.New(0))
	if got := ProcessElapsedTime(step); got != "00:00:00" {
		t.Fatalf("zero step elapsed = %q, want 00:00:00", got)
	}
	if got := ProcessEndTime(step); got == "unknown" {
		t.Fatal("equal step start/end must still display an end time")
	}

	item = accountingStep(protos.JobStatus_Failed, start, start+5,
		durationpb.New(7*time.Second))
	if got := ProcessElapsedTime(item); got != "00:00:07" {
		t.Fatalf("backend elapsed = %q, want 00:00:07", got)
	}
	item.stepInfo.ElapsedTime = &durationpb.Duration{Seconds: 1, Nanos: -1}
	if got := ProcessElapsedTime(item); got != "unknown" {
		t.Fatalf("invalid backend elapsed = %q, want unknown", got)
	}
}

func TestProcessAccountingRejectsMissingAndReversedTimes(t *testing.T) {
	const start = int64(1_700_000_000)
	reversed := accountingJob(protos.JobStatus_Failed, start+5, start, nil)
	if got := ProcessElapsedTime(reversed); got != "unknown" {
		t.Fatalf("reversed elapsed = %q, want unknown", got)
	}
	if got := ProcessEndTime(reversed); got != "unknown" {
		t.Fatalf("reversed end time = %q, want unknown", got)
	}
	reversed.job.ElapsedTime = durationpb.New(5 * time.Second)
	if got := ProcessElapsedTime(reversed); got != "unknown" {
		t.Fatalf("reversed elapsed with backend value = %q, want unknown", got)
	}

	missing := &JobOrStep{job: &protos.JobInfo{
		Status:    protos.JobStatus_Completed,
		StartTime: accountingTimestamp(start),
	}}
	if got := ProcessElapsedTime(missing); got != "unknown" {
		t.Fatalf("missing elapsed = %q, want unknown", got)
	}
	if got := ProcessEndTime(missing); got != "unknown" {
		t.Fatalf("missing end time = %q, want unknown", got)
	}
}

func TestProcessAccountingUsesValidZeroWithoutTimestamps(t *testing.T) {
	item := &JobOrStep{job: &protos.JobInfo{
		Status:      protos.JobStatus_Cancelled,
		ElapsedTime: durationpb.New(0),
	}}
	if got := ProcessElapsedTime(item); got != "00:00:00" {
		t.Fatalf("zero elapsed without timestamps = %q, want 00:00:00", got)
	}
}

func TestProcessEndTimeUsesValidEndWithoutStart(t *testing.T) {
	item := &JobOrStep{job: &protos.JobInfo{
		Status:  protos.JobStatus_Failed,
		EndTime: accountingTimestamp(1_700_000_005),
	}}
	if got := ProcessEndTime(item); got == "unknown" {
		t.Fatal("valid terminal end time should be displayed when start is missing")
	}
}

func TestProcessEndTimePreservesCompletingSemantics(t *testing.T) {
	item := accountingJob(protos.JobStatus_Completing, 1_700_000_000,
		1_700_000_005, nil)
	if got := ProcessEndTime(item); got == "unknown" {
		t.Fatal("completing end time should be displayed")
	}
	item = accountingJob(protos.JobStatus_Completing, 1_700_000_000,
		1_700_000_000, nil)
	if got := ProcessEndTime(item); got != "unknown" {
		t.Fatalf("zero completing end time = %q, want unknown", got)
	}
}

func TestProcessStartAndSubmitTimeHandleMissingTimestamps(t *testing.T) {
	item := &JobOrStep{job: &protos.JobInfo{Status: protos.JobStatus_Failed}}
	if got := ProcessStartTime(item); got != "unknown" {
		t.Fatalf("missing start time = %q, want unknown", got)
	}
	if got := ProcessSubmitTime(item); got != "unknown" {
		t.Fatalf("missing submit time = %q, want unknown", got)
	}
}

func TestProcessAccountingRejectsInvalidAndEarlyTimestamps(t *testing.T) {
	const start = int64(1_700_000_000)
	for name, endTime := range map[string]*timestamppb.Timestamp{
		"invalid": {Seconds: start, Nanos: int32(time.Second)},
		"early":   accountingTimestamp(time.Date(1979, 12, 31, 0, 0, 0, 0, time.UTC).Unix()),
	} {
		t.Run(name, func(t *testing.T) {
			item := accountingJob(protos.JobStatus_Failed, start, start, nil)
			item.job.EndTime = endTime
			if got := ProcessElapsedTime(item); got != "unknown" {
				t.Fatalf("elapsed = %q, want unknown", got)
			}
			if got := ProcessEndTime(item); got != "unknown" {
				t.Fatalf("end time = %q, want unknown", got)
			}
		})
	}
}

func TestProcessAccountingSubsecondFallback(t *testing.T) {
	const seconds = int64(1_700_000_000)
	item := accountingJob(protos.JobStatus_Completed, seconds, seconds, nil)
	item.job.StartTime = timestamppb.New(time.Unix(seconds, int64(100*time.Millisecond)))
	item.job.EndTime = timestamppb.New(time.Unix(seconds, int64(900*time.Millisecond)))
	if got := ProcessElapsedTime(item); got != "00:00:00" {
		t.Fatalf("subsecond elapsed = %q, want 00:00:00", got)
	}
}
