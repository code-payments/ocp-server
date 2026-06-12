package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/ocp/data/task/tests"
)

func TestTaskMemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}
