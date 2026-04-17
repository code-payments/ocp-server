package currency

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	currencypb "github.com/code-payments/ocp-protobuf-api/generated/go/currency/v1"
	"github.com/mr-tron/base58"

	"github.com/code-payments/ocp-server/grpc/client"
	"github.com/code-payments/ocp-server/ocp/common"
	"github.com/code-payments/ocp-server/ocp/config"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/vault"
	vm_metadata "github.com/code-payments/ocp-server/ocp/data/vm/metadata"
	"github.com/code-payments/ocp-server/solana/currencycreator"
	"github.com/code-payments/ocp-server/solana/system"
	timelock_token "github.com/code-payments/ocp-server/solana/timelock/v1"
	"github.com/code-payments/ocp-server/solana/vm"
)

func (s *currencyServer) Launch(ctx context.Context, req *currencypb.LaunchRequest) (*currencypb.LaunchResponse, error) {
	log := s.log.With(zap.String("method", "Launch"))
	log = client.InjectLoggingMetadata(ctx, log)

	ownerAccount, err := common.NewAccountFromProto(req.Owner)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid owner address")
		return nil, status.Error(codes.Internal, "")
	}

	signature := req.Signature
	req.Signature = nil
	err = s.auth.Authenticate(ctx, ownerAccount, req, signature)
	if err != nil {
		return nil, err
	}

	// Do not restrict currency launch internally for now
	isDryRun := false

	count, err := s.data.CountCurrencyMints(ctx)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to count currency mints")
		return nil, status.Error(codes.Internal, "")
	}
	if count > s.conf.maxCurrencyCount.Get(ctx) {
		log.Info("exceeded max currencies")
		return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
	}

	name := strings.TrimSpace(req.Name)
	if isReservedCurrencyName(name) {
		log.Info("denying reserved currency name")
		return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
	}
	if name != req.Name {
		log.Info("name requires whitespace trimming")
		return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
	}

	isModerated, err := s.moderation.ValidateAttestation(ctx, ownerAccount, req.NameModerationAttestation.RawValue, req.Name)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to validate name moderation attestation")
		return nil, status.Error(codes.Internal, "")
	} else if !isModerated {
		log.Info("name is not moderated")
		return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
	}

	symbol := strings.TrimSpace(req.Symbol)
	if len(symbol) == 0 {
		symbol = strings.ToUpper(strings.Map(
			func(r rune) rune {
				if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
					return -1
				}
				return r
			},
			name,
		))
		if len(symbol) > currencycreator.MaxCurrencyConfigAccountSymbolLength {
			symbol = symbol[0:currencycreator.MaxCurrencyConfigAccountSymbolLength]
		}
	} else {
		if symbol != req.Symbol {
			log.Info("symbol requires whitespace trimming")
			return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
		}
		if req.SymbolModerationAttestation == nil {
			log.Info("symbol moderation attestation is missing")
			return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
		}
		isModerated, err := s.moderation.ValidateAttestation(ctx, ownerAccount, req.SymbolModerationAttestation.RawValue, req.Symbol)
		if err != nil {
			log.With(zap.Error(err)).Warn("failed to validate symbol moderation attestation")
			return nil, status.Error(codes.Internal, "")
		} else if !isModerated {
			log.Info("symbol is not moderated")
			return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
		}
	}

	description := strings.TrimSpace(req.Description)
	if len(req.Description) > 0 {
		if req.DescriptionModerationAttestation == nil {
			log.Info("description moderation attestation is missing")
			return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
		}
		isModerated, err := s.moderation.ValidateAttestation(ctx, ownerAccount, req.DescriptionModerationAttestation.RawValue, req.Description)
		if err != nil {
			log.With(zap.Error(err)).Warn("failed to validate description moderation attestation")
			return nil, status.Error(codes.Internal, "")
		} else if !isModerated {
			log.Info("description is not moderated")
			return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
		}
	}

	var processedIcon []byte
	var iconExt, iconContentType string
	if len(req.Icon) > 0 {
		if req.IconModerationAttestation == nil {
			log.Info("icon moderation attestation is missing")
			return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
		}
		isModerated, err := s.moderation.ValidateAttestation(ctx, ownerAccount, req.IconModerationAttestation.RawValue, req.Icon)
		if err != nil {
			log.With(zap.Error(err)).Warn("failed to validate icon moderation attestation")
			return nil, status.Error(codes.Internal, "")
		} else if !isModerated {
			log.Info("icon is not moderated")
			return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
		}

		processedIcon, iconExt, iconContentType, err = processIcon(req.Icon)
		if err == errInvalidIcon {
			return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_INVALID_ICON}, nil
		} else if err != nil {
			log.With(zap.Error(err)).Warn("falied to process icon")
			return nil, status.Error(codes.Internal, "")
		}
	}

	ownerMetadata, err := common.GetOwnerMetadata(ctx, s.data, ownerAccount)
	if err == common.ErrOwnerNotFound {
		log.Info("owner is not an ocp account")
		return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("failure getting owner metadata")
		return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
	}
	if ownerMetadata.Type != common.OwnerTypeUser12Words || ownerMetadata.State != common.OwnerManagementStateOcpAccount {
		log.Info("owner must be a user 12 word account")
		return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
	}

	allow, err := s.antispamGuard.AllowCurrencyLaunch(ctx, ownerAccount, name, symbol)
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to perform antispam checks")
		return nil, status.Error(codes.Internal, "")
	} else if !allow {
		log.Info("antispam guard denied currency launch")
		return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_DENIED}, nil
	}

	authority, err := common.NewRandomAccount()
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to generate authority key")
		return nil, status.Error(codes.Internal, "")
	}

	seed, err := common.NewRandomAccount()
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to generating seed")
		return nil, status.Error(codes.Internal, "")
	}

	targetMintAddress, targetMintBump, err := currencycreator.GetMintAddress(&currencycreator.GetMintAddressArgs{
		Authority: authority.PublicKey().ToBytes(),
		Name:      name,
		Seed:      seed.PublicKey().ToBytes(),
	})
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to derive mint address")
		return nil, status.Error(codes.Internal, "")
	}
	targetMintAccount, err := common.NewAccountFromPublicKeyBytes(targetMintAddress)
	if err != nil {
		log.With(zap.Error(err)).Warn("invalid target mint addres")
		return nil, status.Error(codes.Internal, "")
	}

	currencyAddress, currencyBump, err := currencycreator.GetCurrencyAddress(&currencycreator.GetCurrencyAddressArgs{
		Mint: targetMintAddress,
	})
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to derive currency config address")
		return nil, status.Error(codes.Internal, "")
	}

	poolAddress, poolBump, err := currencycreator.GetPoolAddress(&currencycreator.GetPoolAddressArgs{
		Currency: currencyAddress,
	})
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to derive liquidity pool address")
		return nil, status.Error(codes.Internal, "")
	}

	vaultBaseAddress, vaultBaseBump, err := currencycreator.GetVaultAddress(&currencycreator.GetVaultAddressArgs{
		Pool: poolAddress,
		Mint: common.CoreMintAccount.PublicKey().ToBytes(),
	})
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to derive base vault address")
		return nil, status.Error(codes.Internal, "")
	}

	vaultTargetAddress, vaultTargetBump, err := currencycreator.GetVaultAddress(&currencycreator.GetVaultAddressArgs{
		Pool: poolAddress,
		Mint: targetMintAddress,
	})
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to derive target vault address")
		return nil, status.Error(codes.Internal, "")
	}

	vmAddress, vmBump, err := vm.GetVmAddress(&vm.GetVmAddressArgs{
		Mint:         targetMintAddress,
		VmAuthority:  authority.PublicKey().ToBytes(),
		LockDuration: timelock_token.DefaultNumDaysLocked,
	})
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to derive vm address")
		return nil, status.Error(codes.Internal, "")
	}

	vmOmnibusAddress, vmOmnibusBump, err := vm.GetVmObnibusAddress(&vm.GetVmObnibusAddressArgs{
		Vm: vmAddress,
	})
	if err != nil {
		log.With(zap.Error(err)).Warn("failed to derive vm omnibus address")
		return nil, status.Error(codes.Internal, "")
	}

	creationTs := time.Now()

	authorityVaultRecord := vault.Record{
		PublicKey:  authority.PublicKey().ToBase58(),
		PrivateKey: authority.PrivateKey().ToBase58(),
		State:      vault.StateReserved,
		CreatedAt:  creationTs,
	}

	currencyMetadataRecord := &currency.MetadataRecord{
		Name:        name,
		Symbol:      symbol,
		Description: " ",
		ImageUrl:    config.DefaultCurrencyIconImageUrl,

		Seed: seed.PublicKey().ToBase58(),

		Authority: authority.PublicKey().ToBase58(),

		Mint:     base58.Encode(targetMintAddress),
		MintBump: targetMintBump,
		Decimals: currencycreator.DefaultMintDecimals,

		CurrencyConfig:     base58.Encode(currencyAddress),
		CurrencyConfigBump: currencyBump,

		LiquidityPool:     base58.Encode(poolAddress),
		LiquidityPoolBump: poolBump,

		VaultMint:     base58.Encode(vaultTargetAddress),
		VaultMintBump: vaultTargetBump,

		VaultCore:     base58.Encode(vaultBaseAddress),
		VaultCoreBump: vaultBaseBump,

		SellFeeBps: currencycreator.DefaultSellFeeBps,

		Alt: base58.Encode(system.ProgramKey[:]), // ALT filled in later due to recent slot requirements

		State: currency.MetadataStateWaitingForInitialPurchase,

		CreatedBy: ownerAccount.PublicKey().ToBase58(),
		CreatedAt: creationTs,
	}

	if len(description) > 0 {
		currencyMetadataRecord.Description = description
	}
	if req.BillCustomization != nil {
		var billColors []string
		for _, billColor := range req.BillCustomization.Colors {
			billColors = append(billColors, billColor.Hex)
		}
		currencyMetadataRecord.BillColors = billColors
	}

	vmMetadataRecord := &vm_metadata.Record{
		Mint:        base58.Encode(targetMintAddress),
		Authority:   authority.PublicKey().ToBase58(),
		Vm:          base58.Encode(vmAddress),
		VmBump:      vmBump,
		Omnibus:     base58.Encode(vmOmnibusAddress),
		OmnibusBump: vmOmnibusBump,
		DaysLocked:  timelock_token.DefaultNumDaysLocked,
		State:       vm_metadata.StateUnknown,
		CreatedAt:   creationTs,
	}

	if !isDryRun {
		var iconUploaded bool
		if len(processedIcon) > 0 {
			imageUrl, err := uploadIcon(ctx, s.s3Client, targetMintAccount, processedIcon, iconExt, iconContentType)
			if err != nil {
				log.With(zap.Error(err)).Warn("failed to upload icon to s3")
				return nil, status.Error(codes.Internal, "")
			}
			currencyMetadataRecord.ImageUrl = imageUrl
			iconUploaded = true
		}

		err = s.data.ExecuteInTx(ctx, sql.LevelDefault, func(ctx context.Context) error {
			err := s.data.SaveKey(ctx, &authorityVaultRecord)
			if err != nil {
				return err
			}

			err = s.data.SaveCurrencyMetadata(ctx, currencyMetadataRecord)
			if err != nil {
				return err
			}

			return s.data.SaveVmMetadata(ctx, vmMetadataRecord)
		})
		if err != nil {
			if iconUploaded {
				if err := deleteIcon(context.Background(), s.s3Client, targetMintAccount, iconExt); err != nil {
					log.With(zap.Error(err)).Warn("best-effort icon cleanup failed")
				}
			}

			if err == currency.ErrDuplicateCurrency {
				return &currencypb.LaunchResponse{Result: currencypb.LaunchResponse_NAME_EXISTS}, nil
			}
			log.With(zap.Error(err)).Warn("failed to save currency and vm metadata")
			return nil, status.Error(codes.Internal, "")
		}
	}

	return &currencypb.LaunchResponse{
		Result: currencypb.LaunchResponse_OK,
		Mint:   targetMintAccount.ToProto(),
	}, nil
}
