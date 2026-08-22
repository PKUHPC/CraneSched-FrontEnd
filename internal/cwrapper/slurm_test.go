package cwrapper

import (
	"reflect"
	"testing"
)

func TestNormalizeScontrolShowHostnames(t *testing.T) {
	testCases := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "official spelling",
			args: []string{"show", "hostnames", "node[01-02]"},
			want: []string{"ccontrol", "show", "hostnames", "node[01-02]"},
		},
		{
			name: "supported abbreviation and assignment",
			args: []string{"show", "HostName=node[01-02]"},
			want: []string{"ccontrol", "show", "hostnames", "node[01-02]"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := normalizeScontrolArgs(testCase.args); !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("normalizeScontrolArgs(%q) = %q, want %q", testCase.args, got, testCase.want)
			}
		})
	}
}
