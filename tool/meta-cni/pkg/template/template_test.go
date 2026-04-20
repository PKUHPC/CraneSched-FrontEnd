package template

import (
	"encoding/json"
	"testing"
)

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

func TestRenderSimple(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"type":"sriov","deviceID":"{{.GRES.device}}"}`)
	out, err := Render(raw, BuildVars("0000:86:00.2", "0", nil))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got["deviceID"] != "0000:86:00.2" {
		t.Fatalf("deviceID = %v, want %q", got["deviceID"], "0000:86:00.2")
	}
}

func TestRenderNestedValues(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"type":"plugin",
		"labels":["{{.GRES.index}}","fixed"],
		"nested":{"alias":"{{.ARGS.ALIAS}}"}
	}`)

	out, err := Render(raw, BuildVars("dev0", "2", map[string]string{
		"ALIAS": "rdma2",
	}))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got struct {
		Labels []string          `json:"labels"`
		Nested map[string]string `json:"nested"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got.Labels[0] != "2" {
		t.Fatalf("labels[0] = %q, want %q", got.Labels[0], "2")
	}
	if got.Nested["alias"] != "rdma2" {
		t.Fatalf("nested.alias = %q, want %q", got.Nested["alias"], "rdma2")
	}
}

func TestRenderAllowsBuiltInTemplateSyntax(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"alias":"{{if .ARGS.ALIAS}}{{.ARGS.ALIAS}}{{else}}rdma{{.GRES.index}}{{end}}",
		"dashed":"{{index .ARGS \"KEY-WITH-DASH\"}}"
	}`)

	out, err := Render(raw, BuildVars("dev0", "7", map[string]string{
		"KEY-WITH-DASH": "dash-value",
	}))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got map[string]string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got["alias"] != "rdma7" {
		t.Fatalf("alias = %q, want %q", got["alias"], "rdma7")
	}
	if got["dashed"] != "dash-value" {
		t.Fatalf("dashed = %q, want %q", got["dashed"], "dash-value")
	}
}

func TestRenderKeepsOutputValidJSON(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"note":"{{.ARGS.TEXT}}"}`)
	out, err := Render(raw, BuildVars("", "", map[string]string{
		"TEXT": `a"b\c`,
	}))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got map[string]string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v (output=%s)", err, string(out))
	}
	if got["note"] != `a"b\c` {
		t.Fatalf("note = %q, want %q", got["note"], `a"b\c`)
	}
}

func TestRenderBadTemplateErrors(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"note":"{{if .GRES.device}}"}`)
	_, err := Render(raw, BuildVars("dev0", "0", nil))
	if err == nil {
		t.Fatal("expected parse error")
	}
}
