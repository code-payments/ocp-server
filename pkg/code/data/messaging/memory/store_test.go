package memory

import (
	"testing"

	"github.com/code-payments/ocp-server/pkg/code/data/messaging/tests"
)

func TestMessagingMemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}

	tests.RunTests(t, testStore, teardown)
}
