package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/ocp/data/currency/metadata/tests"
)

func TestMetadata_MemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}
