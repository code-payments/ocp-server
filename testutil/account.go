package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/ocp-server/ocp/common"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
)

func NewRandomAccount(t *testing.T) *common.Account {
	account, err := common.NewRandomAccount()
	require.NoError(t, err)

	return account
}

func SetupRandomSubsidizer(t *testing.T, data ocp_data.Provider) *common.Account {
	account := NewRandomAccount(t)
	require.NoError(t, common.InjectTestSubsidizer(context.Background(), data, account))
	return account
}
