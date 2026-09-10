package transaction

import (
	"context"
	"crypto/ed25519"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"

	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	"github.com/code-payments/ocp-server/solana"
	coinbase_stable_swapper "github.com/code-payments/ocp-server/solana/coinbasestableswapper"
	"github.com/code-payments/ocp-server/solana/token"
)

// CoinbaseSwapAccounts captures all derived PDAs and on-chain values needed
// to build a CoinbaseStableSwapper::Swap instruction for a (fromMint, toMint)
// pair. FeeRecipient is sourced from the on-chain LiquidityPool account.
type CoinbaseSwapAccounts struct {
	Pool                     ed25519.PublicKey
	InVault                  ed25519.PublicKey
	OutVault                 ed25519.PublicKey
	InVaultTokenAccount      ed25519.PublicKey
	OutVaultTokenAccount     ed25519.PublicKey
	Whitelist                ed25519.PublicKey
	FeeRecipient             ed25519.PublicKey
	FeeRecipientTokenAccount ed25519.PublicKey
}

// GetCoinbaseSwapAccounts derives all PDAs and reads the on-chain pool account
// to resolve the fee recipient and its from-mint ATA.
func GetCoinbaseSwapAccounts(ctx context.Context, data ocp_data.Provider, fromMint, toMint ed25519.PublicKey) (*CoinbaseSwapAccounts, error) {
	pool, _, err := coinbase_stable_swapper.GetPoolAddress()
	if err != nil {
		return nil, errors.Wrap(err, "error getting coinbase pool address")
	}

	inVault, _, err := coinbase_stable_swapper.GetTokenVaultAddress(&coinbase_stable_swapper.GetTokenVaultAddressArgs{
		Pool: pool,
		Mint: fromMint,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error getting in vault address")
	}

	outVault, _, err := coinbase_stable_swapper.GetTokenVaultAddress(&coinbase_stable_swapper.GetTokenVaultAddressArgs{
		Pool: pool,
		Mint: toMint,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error getting out vault address")
	}

	inVaultTokenAccount, _, err := coinbase_stable_swapper.GetVaultTokenAccountAddress(&coinbase_stable_swapper.GetVaultTokenAccountAddressArgs{
		Vault: inVault,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error getting in vault token account address")
	}

	outVaultTokenAccount, _, err := coinbase_stable_swapper.GetVaultTokenAccountAddress(&coinbase_stable_swapper.GetVaultTokenAccountAddressArgs{
		Vault: outVault,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error getting out vault token account address")
	}

	whitelist, _, err := coinbase_stable_swapper.GetWhitelistAddress()
	if err != nil {
		return nil, errors.Wrap(err, "error getting whitelist address")
	}

	poolAccountInfo, _, err := data.GetBlockchainAccountInfo(ctx, base58.Encode(pool), solana.CommitmentProcessed)
	if err != nil {
		return nil, errors.Wrap(err, "error getting coinbase liquidity pool account info")
	}

	var poolAccount coinbase_stable_swapper.LiquidityPoolAccount
	if err := poolAccount.Unmarshal(poolAccountInfo.Data); err != nil {
		return nil, errors.Wrap(err, "error unmarshalling coinbase liquidity pool account")
	}

	feeRecipientTokenAccount, err := token.GetAssociatedAccount(poolAccount.FeeRecipient, fromMint)
	if err != nil {
		return nil, errors.Wrap(err, "error getting fee recipient token account")
	}

	return &CoinbaseSwapAccounts{
		Pool:                     pool,
		InVault:                  inVault,
		OutVault:                 outVault,
		InVaultTokenAccount:      inVaultTokenAccount,
		OutVaultTokenAccount:     outVaultTokenAccount,
		Whitelist:                whitelist,
		FeeRecipient:             poolAccount.FeeRecipient,
		FeeRecipientTokenAccount: feeRecipientTokenAccount,
	}, nil
}

// GetCoinbaseSwapDestinationLiquidity returns the balance of the Coinbase
// Stable Swapper's token vault for the destination mint, which represents the
// available liquidity for swaps targeting that mint.
func GetCoinbaseSwapDestinationLiquidity(ctx context.Context, data ocp_data.Provider, toMint ed25519.PublicKey) (uint64, error) {
	pool, _, err := coinbase_stable_swapper.GetPoolAddress()
	if err != nil {
		return 0, errors.Wrap(err, "error getting coinbase pool address")
	}

	outVault, _, err := coinbase_stable_swapper.GetTokenVaultAddress(&coinbase_stable_swapper.GetTokenVaultAddressArgs{
		Pool: pool,
		Mint: toMint,
	})
	if err != nil {
		return 0, errors.Wrap(err, "error getting out vault address")
	}

	outVaultTokenAccount, _, err := coinbase_stable_swapper.GetVaultTokenAccountAddress(&coinbase_stable_swapper.GetVaultTokenAccountAddressArgs{
		Vault: outVault,
	})
	if err != nil {
		return 0, errors.Wrap(err, "error getting out vault token account address")
	}

	info, err := data.GetBlockchainTokenAccountInfo(
		ctx,
		base58.Encode(outVaultTokenAccount),
		base58.Encode(toMint),
		solana.CommitmentProcessed,
	)
	if err != nil {
		return 0, errors.Wrap(err, "error getting out vault token account info")
	}

	return info.Amount, nil
}

// MakeCoinbaseSwapInstruction builds a CoinbaseStableSwapper::Swap instruction.
func MakeCoinbaseSwapInstruction(
	accounts *CoinbaseSwapAccounts,
	user ed25519.PublicKey,
	fromMint, toMint ed25519.PublicKey,
	userFromTokenAccount, userToTokenAccount ed25519.PublicKey,
	amountIn, minAmountOut uint64,
) solana.Instruction {
	return coinbase_stable_swapper.NewSwapInstruction(
		&coinbase_stable_swapper.SwapInstructionAccounts{
			Pool:                     accounts.Pool,
			InVault:                  accounts.InVault,
			OutVault:                 accounts.OutVault,
			InVaultTokenAccount:      accounts.InVaultTokenAccount,
			OutVaultTokenAccount:     accounts.OutVaultTokenAccount,
			UserFromTokenAccount:     userFromTokenAccount,
			ToTokenAccount:           userToTokenAccount,
			FeeRecipientTokenAccount: accounts.FeeRecipientTokenAccount,
			FeeRecipient:             accounts.FeeRecipient,
			FromMint:                 fromMint,
			ToMint:                   toMint,
			User:                     user,
			Whitelist:                accounts.Whitelist,
		},
		&coinbase_stable_swapper.SwapInstructionArgs{
			AmountIn:     amountIn,
			MinAmountOut: minAmountOut,
		},
	)
}
