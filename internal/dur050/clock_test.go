package dur050

import (
	"testing"
	"time"
)

func TestSharedMonotonicNanosecondsDoesNotMoveBackwards(t *testing.T) {
	first, err := SharedMonotonicNanoseconds()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := SharedMonotonicNanoseconds()
	if err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("monotonic clock moved backwards: first=%d second=%d", first, second)
	}
}
