package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/ocp/data/account/tests"
)

func TestAccountInfoMemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}
