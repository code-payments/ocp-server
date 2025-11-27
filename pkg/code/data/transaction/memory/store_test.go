package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/pkg/code/data/transaction/tests"
)

func TestTransactionMemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}
