package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/ocp/data/vm/metadata/tests"
)

func TestVmMetadataMemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}
