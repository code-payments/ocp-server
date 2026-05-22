package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/account/tests"
)

func TestAccountInfoMemoryStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}

func TestLegacyUninitializedBalance(t *testing.T) {
	ctx := context.Background()

	testStore := New()
	record := &account.Record{
		OwnerAccount:     "legacy_owner",
		AuthorityAccount: "legacy_owner",
		TokenAccount:     "legacy_token",
		MintAccount:      "mint",
		AccountType:      commonpb.AccountType_PRIMARY,
	}
	require.NoError(t, testStore.Put(ctx, record))

	// Simulate a legacy pre-migration row by clearing its stored balance.
	for _, r := range testStore.(*store).records {
		if r.TokenAccount == record.TokenAccount {
			r.Balance = nil
		}
	}

	tests.RunUninitializedBalanceTests(t, testStore, record.TokenAccount)
}
