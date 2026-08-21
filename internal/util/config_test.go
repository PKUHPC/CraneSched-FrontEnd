package util

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestConfig(t *testing.T, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}
	return path
}

func TestParseConfigDefaultsTaskIOChannelCapacity(t *testing.T) {
	configPath := writeTestConfig(t, "CraneCtldForInternalListenPort: \"10013\"\n")

	config := ParseConfig(configPath)

	if got := config.Cfored.TaskIOChannelCapacity; got != DefaultCforedTaskIOChannelCapacity {
		t.Fatalf("TaskIOChannelCapacity = %d, want %d", got, DefaultCforedTaskIOChannelCapacity)
	}
}

func TestParseConfigUsesConfiguredTaskIOChannelCapacity(t *testing.T) {
	configPath := writeTestConfig(t, `CraneCtldForInternalListenPort: "10013"
Cfored:
  TaskIOChannelCapacity: 8192
`)

	config := ParseConfig(configPath)

	if got := config.Cfored.TaskIOChannelCapacity; got != 8192 {
		t.Fatalf("TaskIOChannelCapacity = %d, want 8192", got)
	}
}
