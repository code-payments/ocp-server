package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/ocp/data/balance/watch/tests"
)

func TestWatch_MemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}
