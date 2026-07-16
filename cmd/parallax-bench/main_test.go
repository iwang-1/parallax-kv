package main

import (
	"testing"
	"time"
)

func TestKeyForUsesFixedWidth(t *testing.T) {
	for _, i := range []int{0, 999, 999999, int(^uint(0) >> 1), -1} {
		got := keyFor(i)
		if len(got) != keySize {
			t.Fatalf("keyFor(%d) length = %d, want %d: %q", i, len(got), keySize, got)
		}
	}
}

func TestInMeasurementWindow(t *testing.T) {
	base := time.Unix(100, 0)
	measureAt := base.Add(5 * time.Second)
	deadline := measureAt.Add(30 * time.Second)
	tests := []struct {
		name       string
		start, end time.Time
		want       bool
	}{
		{"starts and ends during warmup", base, measureAt, false},
		{"starts during warmup and finishes inside", measureAt.Add(-time.Millisecond), measureAt.Add(time.Millisecond), false},
		{"starts at measurement boundary", measureAt, measureAt.Add(time.Millisecond), true},
		{"fully inside measurement window", measureAt.Add(time.Millisecond), deadline.Add(-time.Millisecond), true},
		{"ends at deadline", deadline.Add(-time.Millisecond), deadline, true},
		{"starts at deadline", deadline, deadline, false},
		{"finishes after deadline", deadline.Add(-time.Millisecond), deadline.Add(time.Millisecond), false},
		{"finishes before it starts", measureAt.Add(time.Millisecond), measureAt, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := inMeasurementWindow(tt.start, tt.end, measureAt, deadline); got != tt.want {
				t.Fatalf("inMeasurementWindow() = %v, want %v", got, tt.want)
			}
		})
	}
}
