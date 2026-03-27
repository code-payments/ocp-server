package currency

import (
	"errors"
	"time"

	"github.com/code-payments/ocp-server/solana/currencycreator"
)

type MetadataState uint8

const (
	MetadataStateUnknown MetadataState = iota
	MetadataStateAvailable
	MetadataStateWaitingForInitialPurchase
	MetadataStateFundingAuthority
	MetadataStateExecutingInitialPurchase
	MetadataStateCompletingInitialization
	MetadataStateFinalValidation
)

type SocialLinkType uint8

const (
	SocialLinkTypeUnknown SocialLinkType = iota
	SocialLinkTypeWebsite
	SocialLinkTypeX
	SocialLinkTypeTelegram
	SocialLinkTypeDiscord
)

type SocialLink struct {
	Type  SocialLinkType `json:"type"`
	Value string         `json:"value"`
}

type ExchangeRateRecord struct {
	Id     uint64
	Time   time.Time
	Rate   float64
	Symbol string
}

type MultiRateRecord struct {
	Time  time.Time
	Rates map[string]float64
}

type MetadataRecord struct {
	Id uint64

	Name        string
	Symbol      string
	Description string
	ImageUrl    string
	BillColors  []string
	SocialLinks []SocialLink

	Seed string

	Authority string

	Mint     string
	MintBump uint8
	Decimals uint8

	CurrencyConfig     string
	CurrencyConfigBump uint8

	LiquidityPool     string
	LiquidityPoolBump uint8

	VaultMint     string
	VaultMintBump uint8

	VaultCore     string
	VaultCoreBump uint8

	SellFeeBps uint16

	Alt string

	State   MetadataState
	Version uint64

	CreatedBy string
	CreatedAt time.Time
}

type ReserveRecord struct {
	Id                uint64
	Mint              string
	SupplyFromBonding uint64
	Slot              uint64 // Not available for historical records
	Time              time.Time
}

type HolderCountRecord struct {
	Id          uint64
	Mint        string
	HolderCount uint64
	Time        time.Time
}

func (m *MetadataRecord) Validate() error {
	if len(m.Name) == 0 {
		return errors.New("name is required")
	}

	if len(m.Symbol) == 0 {
		return errors.New("symbol is required")
	}

	if len(m.Description) == 0 {
		return errors.New("description is required")
	}

	if len(m.ImageUrl) == 0 {
		return errors.New("image url is required")
	}

	if len(m.Seed) == 0 {
		return errors.New("seed is required")
	}

	if len(m.Authority) == 0 {
		return errors.New("authority is required")
	}

	if len(m.Mint) == 0 {
		return errors.New("mint is required")
	}

	if m.MintBump == 0 {
		return errors.New("mint bump is required")
	}

	if m.Decimals != currencycreator.DefaultMintDecimals {
		return errors.New("invalid mint decimals")
	}

	if len(m.CurrencyConfig) == 0 {
		return errors.New("currency config is required")
	}

	if m.CurrencyConfigBump == 0 {
		return errors.New("currency config bump is required")
	}

	if len(m.LiquidityPool) == 0 {
		return errors.New("liquidity pool is required")
	}

	if m.LiquidityPoolBump == 0 {
		return errors.New("liquidity pool bump is required")
	}

	if len(m.VaultMint) == 0 {
		return errors.New("vault mint is required")
	}

	if m.VaultMintBump == 0 {
		return errors.New("vault mint bump is required")
	}

	if len(m.VaultCore) == 0 {
		return errors.New("vault core is required")
	}

	if m.VaultCoreBump == 0 {
		return errors.New("vault core bump is required")
	}

	if len(m.Name) == 0 {
		return errors.New("fees core is required")
	}

	if m.SellFeeBps != currencycreator.DefaultSellFeeBps {
		return errors.New("invalid buy sell bps")
	}

	if len(m.Alt) == 0 {
		return errors.New("alt is required")
	}

	if len(m.CreatedBy) == 0 {
		return errors.New("created by is required")
	}

	if m.CreatedAt.IsZero() {
		return errors.New("creation timestamp is required")
	}

	return nil
}

func (m *MetadataRecord) Clone() *MetadataRecord {
	return &MetadataRecord{
		Id: m.Id,

		Name:        m.Name,
		Symbol:      m.Symbol,
		Description: m.Description,
		ImageUrl:    m.ImageUrl,
		BillColors:  append([]string(nil), m.BillColors...),
		SocialLinks: append([]SocialLink(nil), m.SocialLinks...),

		Seed: m.Seed,

		Authority: m.Authority,

		Mint:     m.Mint,
		MintBump: m.MintBump,
		Decimals: m.Decimals,

		CurrencyConfig:     m.CurrencyConfig,
		CurrencyConfigBump: m.CurrencyConfigBump,

		LiquidityPool:     m.LiquidityPool,
		LiquidityPoolBump: m.LiquidityPoolBump,

		VaultMint:     m.VaultMint,
		VaultMintBump: m.VaultMintBump,

		VaultCore:     m.VaultCore,
		VaultCoreBump: m.VaultCoreBump,

		SellFeeBps: m.SellFeeBps,

		Alt: m.Alt,

		State:   m.State,
		Version: m.Version,

		CreatedBy: m.CreatedBy,
		CreatedAt: m.CreatedAt,
	}
}

func (m *MetadataRecord) CopyTo(dst *MetadataRecord) {
	dst.Id = m.Id

	dst.Name = m.Name
	dst.Symbol = m.Symbol
	dst.Description = m.Description
	dst.ImageUrl = m.ImageUrl
	dst.BillColors = append([]string(nil), m.BillColors...)
	dst.SocialLinks = append([]SocialLink(nil), m.SocialLinks...)

	dst.Seed = m.Seed

	dst.Authority = m.Authority

	dst.Mint = m.Mint
	dst.MintBump = m.MintBump
	dst.Decimals = m.Decimals

	dst.CurrencyConfig = m.CurrencyConfig
	dst.CurrencyConfigBump = m.CurrencyConfigBump

	dst.LiquidityPool = m.LiquidityPool
	dst.LiquidityPoolBump = m.LiquidityPoolBump

	dst.VaultMint = m.VaultMint
	dst.VaultMintBump = m.VaultMintBump

	dst.VaultCore = m.VaultCore
	dst.VaultCoreBump = m.VaultCoreBump

	dst.SellFeeBps = m.SellFeeBps

	dst.Alt = m.Alt

	dst.State = m.State
	dst.Version = m.Version

	dst.CreatedBy = m.CreatedBy
	dst.CreatedAt = m.CreatedAt
}

func (m *ReserveRecord) Validate() error {
	if len(m.Mint) == 0 {
		return errors.New("mint is required")
	}

	if m.Time.IsZero() {
		return errors.New("timestamp is required")
	}

	return nil
}

func (m *ReserveRecord) Clone() *ReserveRecord {
	return &ReserveRecord{
		Id:                m.Id,
		Mint:              m.Mint,
		SupplyFromBonding: m.SupplyFromBonding,
		Slot:              m.Slot,
		Time:              m.Time,
	}
}

func (m *ReserveRecord) CopyTo(dst *ReserveRecord) {
	dst.Id = m.Id
	dst.Mint = m.Mint
	dst.SupplyFromBonding = m.SupplyFromBonding
	dst.Slot = m.Slot
	dst.Time = m.Time
}

func (m *HolderCountRecord) Validate() error {
	if len(m.Mint) == 0 {
		return errors.New("mint is required")
	}

	if m.Time.IsZero() {
		return errors.New("timestamp is required")
	}

	return nil
}

func (m *HolderCountRecord) Clone() *HolderCountRecord {
	return &HolderCountRecord{
		Id:          m.Id,
		Mint:        m.Mint,
		HolderCount: m.HolderCount,
		Time:        m.Time,
	}
}

func (m *HolderCountRecord) CopyTo(dst *HolderCountRecord) {
	dst.Id = m.Id
	dst.Mint = m.Mint
	dst.HolderCount = m.HolderCount
	dst.Time = m.Time
}

func (s MetadataState) String() string {
	switch s {
	case MetadataStateAvailable:
		return "available"
	case MetadataStateWaitingForInitialPurchase:
		return "waiting_for_initial_purchase"
	case MetadataStateFundingAuthority:
		return "funding_authority"
	case MetadataStateExecutingInitialPurchase:
		return "executing_initial_purchase"
	case MetadataStateCompletingInitialization:
		return "completing_initializing"
	case MetadataStateFinalValidation:
		return "final_validation"
	}
	return "unknown"
}
