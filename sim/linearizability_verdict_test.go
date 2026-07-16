package sim

import (
	"strings"
	"testing"

	"github.com/anishathalye/porcupine"
)

func TestLinearizabilityVerdictMapping(t *testing.T) {
	s := &Simulator{cfg: Config{Seed: 0xBEEF}}
	tests := []struct {
		name    string
		result  porcupine.CheckResult
		wantErr string
	}{
		{name: "ok", result: porcupine.Ok},
		{name: "unknown", result: porcupine.Unknown, wantErr: "inconclusive"},
		{name: "illegal", result: porcupine.Illegal, wantErr: "violated"},
	}

	errors := make(map[porcupine.CheckResult]string)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.linearizabilityVerdictError(test.result)
			if test.result == porcupine.Ok {
				if err != nil {
					t.Fatalf("Ok returned error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("%v returned nil", test.result)
			}
			message := err.Error()
			if !strings.Contains(message, test.wantErr) {
				t.Fatalf("error %q does not contain %q", message, test.wantErr)
			}
			if !strings.Contains(message, "REPLAY:") || !strings.Contains(message, "0xbeef") {
				t.Fatalf("error is not replay-tagged: %v", err)
			}
			errors[test.result] = message
		})
	}

	if errors[porcupine.Unknown] == errors[porcupine.Illegal] {
		t.Fatalf("Unknown and Illegal mapped to the same error: %q", errors[porcupine.Unknown])
	}
}
