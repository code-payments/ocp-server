package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/pkg/code/data/cvm/storage/tests"
)

func TestVmStorageMemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}
