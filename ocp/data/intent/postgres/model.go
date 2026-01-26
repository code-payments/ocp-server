package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/ocp/config"
	"github.com/code-payments/ocp-server/ocp/data/intent"

	pgutil "github.com/code-payments/ocp-server/database/postgres"
	q "github.com/code-payments/ocp-server/database/query"
)

const (
	intentTableName   = "ocp__core_intent"
	accountsTableName = "ocp__core_intentaccountmetadata"
)

type intentModel struct {
	Id                      sql.NullInt64  `db:"id"`
	IntentId                string         `db:"intent_id"`
	IntentType              uint           `db:"intent_type"`
	Mint                    sql.NullString `db:"mint"`
	InitiatorOwner          string         `db:"owner"`
	Source                  string         `db:"source"`
	DestinationOwnerAccount string         `db:"destination_owner"`
	DestinationTokenAccount string         `db:"destination"`
	Quantity                uint64         `db:"quantity"`
	ExchangeCurrency        string         `db:"exchange_currency"`
	ExchangeRate            float64        `db:"exchange_rate"`
	NativeAmount            float64        `db:"native_amount"`
	UsdMarketValue          float64        `db:"usd_market_value"`
	IsWithdrawal            bool           `db:"is_withdraw"`
	IsRemoteSend            bool           `db:"is_remote_send"`
	IsReturned              bool           `db:"is_returned"`
	IsIssuerVoidingGiftCard bool           `db:"is_issuer_voiding_gift_card"`
	IsSwap                  bool           `db:"is_swap"`
	State                   uint           `db:"state"`
	Version                 int64          `db:"version"`
	CreatedAt               time.Time      `db:"created_at"`

	Accounts []*intentAccountModel
}

func toIntentModel(obj *intent.Record) (*intentModel, error) {
	if err := obj.Validate(); err != nil {
		return nil, err
	}

	if obj.CreatedAt.IsZero() {
		obj.CreatedAt = time.Now().UTC()
	}

	// For backwards compatibility
	var mint sql.NullString
	if obj.MintAccount != config.CoreMintPublicKeyString {
		mint.Valid = true
		mint.String = obj.MintAccount
	}

	m := &intentModel{
		Id:             sql.NullInt64{Int64: int64(obj.Id), Valid: true},
		IntentId:       obj.IntentId,
		IntentType:     uint(obj.IntentType),
		Mint:           mint,
		InitiatorOwner: obj.InitiatorOwnerAccount,
		State:          uint(obj.State),
		CreatedAt:      obj.CreatedAt,
		Version:        int64(obj.Version),
	}

	switch obj.IntentType {
	case intent.OpenAccounts:
	case intent.ExternalDeposit:
		m.DestinationTokenAccount = obj.ExternalDepositMetadata.DestinationTokenAccount
		m.Quantity = obj.ExternalDepositMetadata.Quantity

		m.ExchangeCurrency = string(obj.ExternalDepositMetadata.ExchangeCurrency)
		m.ExchangeRate = obj.ExternalDepositMetadata.ExchangeRate
		m.NativeAmount = obj.ExternalDepositMetadata.NativeAmount
		m.UsdMarketValue = obj.ExternalDepositMetadata.UsdMarketValue

		m.IsSwap = obj.ExternalDepositMetadata.IsSwapBuy
	case intent.SendPublicPayment:
		m.DestinationOwnerAccount = obj.SendPublicPaymentMetadata.DestinationOwnerAccount
		m.DestinationTokenAccount = obj.SendPublicPaymentMetadata.DestinationTokenAccount
		m.Quantity = obj.SendPublicPaymentMetadata.Quantity

		m.ExchangeCurrency = strings.ToLower(string(obj.SendPublicPaymentMetadata.ExchangeCurrency))
		m.ExchangeRate = obj.SendPublicPaymentMetadata.ExchangeRate
		m.NativeAmount = obj.SendPublicPaymentMetadata.NativeAmount
		m.UsdMarketValue = obj.SendPublicPaymentMetadata.UsdMarketValue

		m.IsWithdrawal = obj.SendPublicPaymentMetadata.IsWithdrawal
		m.IsRemoteSend = obj.SendPublicPaymentMetadata.IsRemoteSend
		m.IsSwap = obj.SendPublicPaymentMetadata.IsSwapSell
	case intent.ReceivePaymentsPublicly:
		m.Source = obj.ReceivePaymentsPubliclyMetadata.Source
		m.Quantity = obj.ReceivePaymentsPubliclyMetadata.Quantity

		m.IsRemoteSend = obj.ReceivePaymentsPubliclyMetadata.IsRemoteSend
		m.IsReturned = obj.ReceivePaymentsPubliclyMetadata.IsReturned
		m.IsIssuerVoidingGiftCard = obj.ReceivePaymentsPubliclyMetadata.IsIssuerVoidingGiftCard

		m.ExchangeCurrency = strings.ToLower(string(obj.ReceivePaymentsPubliclyMetadata.OriginalExchangeCurrency))
		m.ExchangeRate = obj.ReceivePaymentsPubliclyMetadata.OriginalExchangeRate
		m.NativeAmount = obj.ReceivePaymentsPubliclyMetadata.OriginalNativeAmount

		m.UsdMarketValue = obj.ReceivePaymentsPubliclyMetadata.UsdMarketValue
	case intent.PublicDistribution:
		m.Source = obj.PublicDistributionMetadata.Source
		m.Quantity = obj.PublicDistributionMetadata.Quantity
		m.UsdMarketValue = obj.PublicDistributionMetadata.UsdMarketValue

		for _, distribution := range obj.PublicDistributionMetadata.Distributions {
			m.Accounts = append(m.Accounts, fromDistribution(m.Id.Int64, distribution))
		}
	default:
		return nil, errors.New("unsupported intent type")
	}

	return m, nil
}

func fromIntentModel(obj *intentModel) *intent.Record {
	record := &intent.Record{
		Id:                    uint64(obj.Id.Int64),
		IntentId:              obj.IntentId,
		IntentType:            intent.Type(obj.IntentType),
		InitiatorOwnerAccount: obj.InitiatorOwner,
		State:                 intent.State(obj.State),
		Version:               uint64(obj.Version),
		CreatedAt:             obj.CreatedAt.UTC(),
	}

	// For backwards compatibility
	if obj.Mint.Valid {
		record.MintAccount = obj.Mint.String
	} else {
		record.MintAccount = config.CoreMintPublicKeyString
	}

	switch record.IntentType {
	case intent.OpenAccounts:
		record.OpenAccountsMetadata = &intent.OpenAccountsMetadata{}
	case intent.ExternalDeposit:
		record.ExternalDepositMetadata = &intent.ExternalDepositMetadata{
			DestinationTokenAccount: obj.DestinationTokenAccount,
			Quantity:                obj.Quantity,

			ExchangeCurrency: currency.Code(obj.ExchangeCurrency),
			ExchangeRate:     obj.ExchangeRate,
			NativeAmount:     obj.NativeAmount,
			UsdMarketValue:   obj.UsdMarketValue,

			IsSwapBuy: obj.IsSwap,
		}

		if len(record.ExternalDepositMetadata.ExchangeCurrency) == 0 {
			record.ExternalDepositMetadata.ExchangeCurrency = currency.USD
			record.ExternalDepositMetadata.NativeAmount = record.ExternalDepositMetadata.UsdMarketValue
		}
	case intent.SendPublicPayment:
		record.SendPublicPaymentMetadata = &intent.SendPublicPaymentMetadata{
			DestinationOwnerAccount: obj.DestinationOwnerAccount,
			DestinationTokenAccount: obj.DestinationTokenAccount,
			Quantity:                obj.Quantity,

			ExchangeCurrency: currency.Code(obj.ExchangeCurrency),
			ExchangeRate:     obj.ExchangeRate,
			NativeAmount:     obj.NativeAmount,
			UsdMarketValue:   obj.UsdMarketValue,

			IsWithdrawal: obj.IsWithdrawal,
			IsRemoteSend: obj.IsRemoteSend,
			IsSwapSell:   obj.IsSwap,
		}
	case intent.ReceivePaymentsPublicly:
		record.ReceivePaymentsPubliclyMetadata = &intent.ReceivePaymentsPubliclyMetadata{
			Source:   obj.Source,
			Quantity: obj.Quantity,

			IsRemoteSend:            obj.IsRemoteSend,
			IsReturned:              obj.IsReturned,
			IsIssuerVoidingGiftCard: obj.IsIssuerVoidingGiftCard,

			OriginalExchangeCurrency: currency.Code(obj.ExchangeCurrency),
			OriginalExchangeRate:     obj.ExchangeRate,
			OriginalNativeAmount:     obj.NativeAmount,

			UsdMarketValue: obj.UsdMarketValue,
		}
	case intent.PublicDistribution:
		record.PublicDistributionMetadata = &intent.PublicDistributionMetadata{
			Source:         obj.Source,
			Quantity:       obj.Quantity,
			UsdMarketValue: obj.UsdMarketValue,
		}

		for _, account := range obj.Accounts {
			record.PublicDistributionMetadata.Distributions = append(record.PublicDistributionMetadata.Distributions, toDistribution(account))
		}
	}

	return record
}

func (m *intentModel) dbSave(ctx context.Context, db *sqlx.DB) error {
	canInsertAccounts := m.Id.Int64 == 0 && len(m.Accounts) > 0

	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `INSERT INTO ` + intentTableName + `
			(intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, state, version, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19 + 1, $20)

			ON CONFLICT (intent_id)
			DO UPDATE
				SET state = $18, version = ` + intentTableName + `.version + 1
				WHERE ` + intentTableName + `.intent_id = $1 AND ` + intentTableName + `.version = $19

			RETURNING
				id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, state, version, created_at`

		err := tx.QueryRowxContext(
			ctx,
			query,
			m.IntentId,
			m.IntentType,
			m.Mint,
			m.InitiatorOwner,
			m.Source,
			m.DestinationOwnerAccount,
			m.DestinationTokenAccount,
			m.Quantity,
			m.ExchangeCurrency,
			m.ExchangeRate,
			m.NativeAmount,
			m.UsdMarketValue,
			m.IsWithdrawal,
			m.IsRemoteSend,
			m.IsReturned,
			m.IsIssuerVoidingGiftCard,
			m.IsSwap,
			m.State,
			m.Version,
			m.CreatedAt,
		).StructScan(m)
		if err != nil {
			return pgutil.CheckNoRows(err, intent.ErrStaleVersion)
		}

		if canInsertAccounts {
			for _, account := range m.Accounts {
				account.PagingId = m.Id.Int64
			}

			_, err = dbBatchPutAccounts(ctx, tx, m.Accounts)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

type intentAccountModel struct {
	Id sql.NullInt64 `db:"id"`

	PagingId int64 `db:"paging_id"`

	Source      sql.NullString `db:"source"`
	SourceOwner sql.NullString `db:"source_owner"`

	Destination      sql.NullString `db:"destination"`
	DestinationOwner sql.NullString `db:"destination_owner"`

	Quantity uint64 `db:"quantity"`
}

func fromDistribution(pagingID int64, obj *intent.Distribution) *intentAccountModel {
	return &intentAccountModel{
		PagingId: pagingID,

		Destination: sql.NullString{
			Valid:  true,
			String: obj.DestinationTokenAccount,
		},
		DestinationOwner: sql.NullString{
			Valid:  true,
			String: obj.DestinationOwnerAccount,
		},

		Quantity: obj.Quantity,
	}
}

func toDistribution(obj *intentAccountModel) *intent.Distribution {
	return &intent.Distribution{
		DestinationOwnerAccount: obj.DestinationOwner.String,
		DestinationTokenAccount: obj.Destination.String,
		Quantity:                obj.Quantity,
	}
}

func dbBatchPutAccounts(ctx context.Context, tx *sqlx.Tx, models []*intentAccountModel) ([]*intentAccountModel, error) {
	var res []*intentAccountModel

	query := `INSERT INTO ` + accountsTableName + `
			(paging_id, source, source_owner, destination, destination_owner, quantity)
			VALUES `
	var parameters []any

	for i, m := range models {
		baseIndex := len(parameters)
		query += fmt.Sprintf(
			`($%d, $%d, $%d, $%d, $%d, $%d)`,
			baseIndex+1, baseIndex+2, baseIndex+3, baseIndex+4, baseIndex+5, baseIndex+6,
		)
		if i != len(models)-1 {
			query += ","
		}

		parameters = append(
			parameters,
			m.PagingId,
			m.Source,
			m.SourceOwner,
			m.Destination,
			m.DestinationOwner,
			m.Quantity,
		)
	}
	query += ` RETURNING id, paging_id, source, source_owner, destination, destination_owner, quantity`

	err := tx.SelectContext(
		ctx,
		&res,
		query,
		parameters...,
	)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func dbGetAccounts(ctx context.Context, db *sqlx.DB, intentType intent.Type, pagingID int64) ([]*intentAccountModel, error) {
	var res []*intentAccountModel
	if intentType != intent.PublicDistribution {
		return res, nil
	}

	query := `SELECT id, paging_id, source, source_owner, destination, destination_owner, quantity
		FROM ` + accountsTableName + `
		WHERE paging_id = $1
		ORDER BY id ASC`

	err := db.SelectContext(ctx, &res, query, pagingID)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func dbGetIntentByIntentID(ctx context.Context, db *sqlx.DB, intentID string) (*intentModel, error) {
	res := &intentModel{}

	query := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, state, version, created_at
		FROM ` + intentTableName + `
		WHERE intent_id = $1
		LIMIT 1`

	err := db.GetContext(ctx, res, query, intentID)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, intent.ErrIntentNotFound)
	}

	res.Accounts, err = dbGetAccounts(ctx, db, intent.Type(res.IntentType), res.Id.Int64)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func dbGetIntentByID(ctx context.Context, db *sqlx.DB, id int64) (*intentModel, error) {
	res := &intentModel{}

	query := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, state, version, created_at
		FROM ` + intentTableName + `
		WHERE id = $1
		LIMIT 1`

	err := db.GetContext(ctx, res, query, id)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, intent.ErrIntentNotFound)
	}

	res.Accounts, err = dbGetAccounts(ctx, db, intent.Type(res.IntentType), res.Id.Int64)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// todo: lots of opportunities for optimizations
func dbGetAllByOwner(ctx context.Context, db *sqlx.DB, owner string, cursor q.Cursor, limit uint64, direction q.Ordering) ([]*intentModel, error) {
	models := []*intentModel{}

	opts := []any{owner}
	query1 := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, state, version, created_at
		FROM ` + intentTableName + `
		WHERE (owner = $1 OR destination_owner = $1)
	`
	query1, opts = q.PaginateQuery(query1, opts, cursor, limit, direction)
	err := db.SelectContext(ctx, &models, query1, opts...)
	if err != nil && !pgutil.IsNoRows(err) {
		return nil, err
	}

	otherIntentRecordIds := []int64{}
	opts = []any{owner}
	query2 := `SELECT paging_id
		FROM ` + accountsTableName + `
		WHERE (source_owner = $1 OR destination_owner = $1)`
	query2, opts = q.PaginateQueryOnField(query2, opts, cursor, limit, direction, "paging_id")
	err = db.SelectContext(ctx, &otherIntentRecordIds, query2, opts...)
	if err != nil && !pgutil.IsNoRows(err) {
		return nil, err
	}

	if len(models) == 0 && len(otherIntentRecordIds) == 0 {
		return nil, intent.ErrIntentNotFound
	}

	var intentRecordIds []int64
	modelsByID := make(map[int64]*intentModel)
	for _, model := range models {
		modelsByID[model.Id.Int64] = model
		intentRecordIds = append(intentRecordIds, model.Id.Int64)
	}
	for _, id := range otherIntentRecordIds {
		if _, ok := modelsByID[id]; !ok {
			modelsByID[id] = nil
			intentRecordIds = append(intentRecordIds, id)
		}
	}

	sort.Slice(intentRecordIds, func(i, j int) bool {
		if direction == q.Ascending {
			return intentRecordIds[i] < intentRecordIds[j]
		}
		return intentRecordIds[j] < intentRecordIds[i]
	})
	if len(intentRecordIds) > int(limit) {
		intentRecordIds = intentRecordIds[:limit]
	}

	res := make([]*intentModel, len(intentRecordIds))
	for i, id := range intentRecordIds {
		res[i] = modelsByID[id]
		if res[i] == nil {
			res[i], err = dbGetIntentByID(ctx, db, id)
			if err != nil {
				return nil, err
			}
		} else {
			res[i].Accounts, err = dbGetAccounts(ctx, db, intent.Type(res[i].IntentType), res[i].Id.Int64)
			if err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}

func dbGetOriginalGiftCardIssuedIntent(ctx context.Context, db *sqlx.DB, giftCardVault string) (*intentModel, error) {
	res := []*intentModel{}

	query := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, state, version, created_at
		FROM ` + intentTableName + `
		WHERE destination = $1 and intent_type = $2 AND state != $3 AND is_remote_send IS TRUE
		LIMIT 2
	`

	err := db.SelectContext(
		ctx,
		&res,
		query,
		giftCardVault,
		intent.SendPublicPayment,
		intent.StateRevoked,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, intent.ErrIntentNotFound)
	}

	if len(res) == 0 {
		return nil, intent.ErrIntentNotFound
	}

	if len(res) > 1 {
		return nil, intent.ErrMultilpeIntentsFound
	}

	return res[0], nil
}

func dbGetGiftCardClaimedIntent(ctx context.Context, db *sqlx.DB, giftCardVault string) (*intentModel, error) {
	res := []*intentModel{}

	query := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, state, version, created_at
		FROM ` + intentTableName + `
		WHERE source = $1 and intent_type = $2 AND state != $3 AND is_remote_send IS TRUE
		LIMIT 2
	`

	err := db.SelectContext(
		ctx,
		&res,
		query,
		giftCardVault,
		intent.ReceivePaymentsPublicly,
		intent.StateRevoked,
	)
	if err != nil {
		return nil, pgutil.CheckNoRows(err, intent.ErrIntentNotFound)
	}

	if len(res) == 0 {
		return nil, intent.ErrIntentNotFound
	}

	if len(res) > 1 {
		return nil, intent.ErrMultilpeIntentsFound
	}

	return res[0], nil
}

func dbGetTransactedAmountForAntiMoneyLaundering(ctx context.Context, db *sqlx.DB, owner string, since time.Time) (uint64, float64, error) {
	res := struct {
		TotalQuarkValue     sql.NullInt64   `db:"total_quark_value"`
		TotalUsdMarketValue sql.NullFloat64 `db:"total_usd_value"`
	}{}

	query := `SELECT SUM(quantity) AS total_quark_value, SUM(usd_market_value) AS total_usd_value FROM ` + intentTableName + `
		WHERE owner = $1 AND created_at >= $2 AND intent_type = $3 AND state != $4 AND is_withdraw = FALSE
	`
	err := db.GetContext(
		ctx,
		&res,
		query,
		owner,
		since,
		intent.SendPublicPayment,
		intent.StateRevoked,
	)
	if err != nil {
		return 0, 0, err
	}

	if !res.TotalQuarkValue.Valid || !res.TotalUsdMarketValue.Valid {
		return 0, 0, nil
	}
	return uint64(res.TotalQuarkValue.Int64), res.TotalUsdMarketValue.Float64, nil
}

func dbGetUsdCostBasis(ctx context.Context, db *sqlx.DB, owner string, mint string) (float64, error) {
	var res sql.NullFloat64

	// For backwards compatibility, the mint column is NULL for core mint
	var mintFilter string
	var params []any
	if mint == config.CoreMintPublicKeyString {
		mintFilter = "mint IS NULL"
		params = []any{owner, intent.StateRevoked, intent.ExternalDeposit, intent.ReceivePaymentsPublicly, intent.SendPublicPayment}
	} else {
		mintFilter = "mint = $6"
		params = []any{owner, intent.StateRevoked, intent.ExternalDeposit, intent.ReceivePaymentsPublicly, intent.SendPublicPayment, mint}
	}

	// USD received as destination:
	//   - ExternalDeposit, ReceivePaymentsPublicly where owner is initiator
	//   - SendPublicPayment where owner is destination_owner
	// USD sent as source:
	//   - SendPublicPayment where owner is initiator
	query := fmt.Sprintf(`SELECT
		(SELECT COALESCE(SUM(usd_market_value), 0) FROM %s WHERE owner = $1 AND %s AND state != $2 AND intent_type IN ($3, $4)) +
		(SELECT COALESCE(SUM(usd_market_value), 0) FROM %s WHERE destination_owner = $1 AND %s AND state != $2 AND intent_type = $5) -
		(SELECT COALESCE(SUM(usd_market_value), 0) FROM %s WHERE owner = $1 AND %s AND state != $2 AND intent_type = $5);`,
		intentTableName, mintFilter,
		intentTableName, mintFilter,
		intentTableName, mintFilter,
	)

	err := db.GetContext(ctx, &res, query, params...)
	if err != nil {
		return 0, err
	}

	if !res.Valid {
		return 0, nil
	}
	return res.Float64, nil
}
