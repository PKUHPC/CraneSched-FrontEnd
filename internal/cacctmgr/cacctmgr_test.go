package cacctmgr

import (
	"CraneFrontEnd/internal/util"
	"reflect"
	"testing"
)

func TestResolveNodeAliasesAcceptsOnlyNodeNameAndNodeHostname(t *testing.T) {
	configNodes := []util.ConfigNodesList{{
		Name:         "crnd1",
		NodeHostname: "host01.example.com",
	}}

	resolved, missing, err := resolveNodeAliases(
		configNodes, []string{"crnd1", "host01.example.com", "host01"})
	if err != nil {
		t.Fatalf("resolveNodeAliases returned error: %v", err)
	}
	if want := []string{"crnd1", "crnd1"}; !reflect.DeepEqual(resolved, want) {
		t.Errorf("resolved aliases = %v, want %v", resolved, want)
	}
	if want := []string{"host01"}; !reflect.DeepEqual(missing, want) {
		t.Errorf("missing aliases = %v, want %v", missing, want)
	}
}
