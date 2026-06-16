package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/ocp/data/currency/holder/tests"
)

func TestHolder_MemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
