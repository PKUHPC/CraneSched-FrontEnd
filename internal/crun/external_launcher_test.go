package crun

import "testing"

func TestSlurmExternalLauncherFromEnv(t *testing.T) {
	for _, value := range []string{"1", "true", "yes", " launcher "} {
		t.Setenv("SLURM_EXTERNAL_LAUNCHER", value)
		if !slurmExternalLauncherFromEnv() {
			t.Errorf("value %q was not recognized", value)
		}
	}
	for _, value := range []string{"", "0", "false", "no"} {
		t.Setenv("SLURM_EXTERNAL_LAUNCHER", value)
		if slurmExternalLauncherFromEnv() {
			t.Errorf("value %q was recognized", value)
		}
	}
	t.Setenv("SLURM_EXTERNAL_LAUNCHER", "")
	if slurmExternalLauncherFromEnv() {
		t.Error("unset marker was recognized")
	}
}
