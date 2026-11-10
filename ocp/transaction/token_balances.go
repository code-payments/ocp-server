package transaction

import (
	"strconv"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/solana"
)

func GetDeltaQuarksFromTokenBalances(tokenAccount *common.Account, tokenBalances *solana.TransactionTokenBalances) (int64, error) {
	var preQuarkBalance, postQuarkBalance int64
	var err error
	for _, tokenBalance := range tokenBalances.PreTokenBalances {
		if tokenBalances.Accounts[tokenBalance.AccountIndex] == tokenAccount.PublicKey().ToBase58() {
			preQuarkBalance, err = strconv.ParseInt(tokenBalance.TokenAmount.Amount, 10, 64)
			if err != nil {
				return 0, errors.Wrap(err, "error parsing pre token balance")
			}
			break
		}
	}
	for _, tokenBalance := range tokenBalances.PostTokenBalances {
		if tokenBalances.Accounts[tokenBalance.AccountIndex] == tokenAccount.PublicKey().ToBase58() {
			postQuarkBalance, err = strconv.ParseInt(tokenBalance.TokenAmount.Amount, 10, 64)
			if err != nil {
				return 0, errors.Wrap(err, "error parsing post token balance")
			}
			break
		}
	}

	return postQuarkBalance - preQuarkBalance, nil
}

// GetPreQuarksFromTokenBalances returns the token account's balance before the
// transaction. It returns false when the account didn't exist before the
// transaction, such as when the transaction created it.
func GetPreQuarksFromTokenBalances(tokenAccount *common.Account, tokenBalances *solana.TransactionTokenBalances) (uint64, bool, error) {
	return getQuarksFromTokenBalances(tokenAccount, tokenBalances.Accounts, tokenBalances.PreTokenBalances)
}

// GetPostQuarksFromTokenBalances returns the token account's balance after the
// transaction. It returns false when the account doesn't exist after the
// transaction.
func GetPostQuarksFromTokenBalances(tokenAccount *common.Account, tokenBalances *solana.TransactionTokenBalances) (uint64, bool, error) {
	return getQuarksFromTokenBalances(tokenAccount, tokenBalances.Accounts, tokenBalances.PostTokenBalances)
}

func getQuarksFromTokenBalances(tokenAccount *common.Account, accounts []string, balances []solana.TokenBalance) (uint64, bool, error) {
	for _, tokenBalance := range balances {
		if accounts[tokenBalance.AccountIndex] == tokenAccount.PublicKey().ToBase58() {
			balance, err := strconv.ParseUint(tokenBalance.TokenAmount.Amount, 10, 64)
			if err != nil {
				return 0, false, errors.Wrap(err, "error parsing token balance")
			}
			return balance, true, nil
		}
	}
	return 0, false, nil
}
