package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/ocp/data/intent/tests"
)

func TestIntentMemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}
