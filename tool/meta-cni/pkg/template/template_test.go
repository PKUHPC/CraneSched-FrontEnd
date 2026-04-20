package template

import "testing"

func TestBuildVars(t *testing.T) {
	t.Parallel()

	vars := BuildVars("0000:86:00.2", "3", map[string]string{
		"MY_KEY": "my_value",
	})

	if vars.GRES["device"] != "0000:86:00.2" {
		t.Fatalf("GRES.device = %q, want %q", vars.GRES["device"], "0000:86:00.2")
	}
	if vars.GRES["index"] != "3" {
		t.Fatalf("GRES.index = %q, want %q", vars.GRES["index"], "3")
	}
	if vars.ARGS["MY_KEY"] != "my_value" {
		t.Fatalf("ARGS.MY_KEY = %q, want %q", vars.ARGS["MY_KEY"], "my_value")
	}
}
