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
	OriginalUsdMarketValue  float64        `db:"original_usd_market_value"`
	IsWithdrawal            bool           `db:"is_withdraw"`
	IsIndirectSend          bool           `db:"is_remote_send"` // todo: rename column
	IsReturned              bool           `db:"is_returned"`
	IsIssuerVoidingGiftCard bool           `db:"is_issuer_voiding_gift_card"`
	IsSwap                  bool           `db:"is_swap"`
	AppMetadata             []byte         `db:"app_metadata"`
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
		AppMetadata:    obj.AppMetadata,
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
		m.OriginalUsdMarketValue = obj.ExternalDepositMetadata.UsdMarketValue

		m.IsSwap = obj.ExternalDepositMetadata.IsSwapBuy
		m.IsReturned = obj.ExternalDepositMetadata.IsReturned
	case intent.SendPublicPayment:
		m.DestinationOwnerAccount = obj.SendPublicPaymentMetadata.DestinationOwnerAccount
		m.DestinationTokenAccount = obj.SendPublicPaymentMetadata.DestinationTokenAccount
		m.Quantity = obj.SendPublicPaymentMetadata.Quantity

		m.ExchangeCurrency = strings.ToLower(string(obj.SendPublicPaymentMetadata.ExchangeCurrency))
		m.ExchangeRate = obj.SendPublicPaymentMetadata.ExchangeRate
		m.NativeAmount = obj.SendPublicPaymentMetadata.NativeAmount
		m.UsdMarketValue = obj.SendPublicPaymentMetadata.UsdMarketValue
		m.OriginalUsdMarketValue = obj.SendPublicPaymentMetadata.UsdMarketValue

		m.IsWithdrawal = obj.SendPublicPaymentMetadata.IsWithdrawal
		m.IsIndirectSend = obj.SendPublicPaymentMetadata.IsIndirectSend
		m.IsSwap = obj.SendPublicPaymentMetadata.IsSwapSell
	case intent.ReceivePaymentsPublicly:
		m.Source = obj.ReceivePaymentsPubliclyMetadata.Source
		m.Quantity = obj.ReceivePaymentsPubliclyMetadata.Quantity

		m.IsIndirectSend = obj.ReceivePaymentsPubliclyMetadata.IsIndirectSend
		m.IsReturned = obj.ReceivePaymentsPubliclyMetadata.IsReturned
		m.IsIssuerVoidingGiftCard = obj.ReceivePaymentsPubliclyMetadata.IsIssuerVoidingGiftCard

		m.ExchangeCurrency = strings.ToLower(string(obj.ReceivePaymentsPubliclyMetadata.OriginalExchangeCurrency))
		m.ExchangeRate = obj.ReceivePaymentsPubliclyMetadata.OriginalExchangeRate
		m.NativeAmount = obj.ReceivePaymentsPubliclyMetadata.OriginalNativeAmount

		m.UsdMarketValue = obj.ReceivePaymentsPubliclyMetadata.UsdMarketValue
		m.OriginalUsdMarketValue = obj.ReceivePaymentsPubliclyMetadata.UsdMarketValue
	case intent.PublicDistribution:
		m.Source = obj.PublicDistributionMetadata.Source
		m.Quantity = obj.PublicDistributionMetadata.Quantity
		m.UsdMarketValue = obj.PublicDistributionMetadata.UsdMarketValue
		m.OriginalUsdMarketValue = obj.PublicDistributionMetadata.UsdMarketValue

		for _, distribution := range obj.PublicDistributionMetadata.Distributions {
			m.Accounts = append(m.Accounts, fromDistribution(m.Id.Int64, distribution))
		}
	case intent.SendPublicMultiPayment:
		m.Source = obj.SendPublicMultiPaymentMetadata.Source
		m.Quantity = obj.SendPublicMultiPaymentMetadata.Quantity

		m.ExchangeCurrency = strings.ToLower(string(obj.SendPublicMultiPaymentMetadata.ExchangeCurrency))
		m.ExchangeRate = obj.SendPublicMultiPaymentMetadata.ExchangeRate
		m.NativeAmount = obj.SendPublicMultiPaymentMetadata.NativeAmount
		m.UsdMarketValue = obj.SendPublicMultiPaymentMetadata.UsdMarketValue
		m.OriginalUsdMarketValue = obj.SendPublicMultiPaymentMetadata.UsdMarketValue

		for _, payment := range obj.SendPublicMultiPaymentMetadata.Payments {
			m.Accounts = append(m.Accounts, fromPayment(m.Id.Int64, payment))
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
		AppMetadata:           obj.AppMetadata,
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

			IsSwapBuy:  obj.IsSwap,
			IsReturned: obj.IsReturned,
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

			IsWithdrawal:   obj.IsWithdrawal,
			IsIndirectSend: obj.IsIndirectSend,
			IsSwapSell:     obj.IsSwap,
		}
	case intent.ReceivePaymentsPublicly:
		record.ReceivePaymentsPubliclyMetadata = &intent.ReceivePaymentsPubliclyMetadata{
			Source:   obj.Source,
			Quantity: obj.Quantity,

			IsIndirectSend:          obj.IsIndirectSend,
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
	case intent.SendPublicMultiPayment:
		record.SendPublicMultiPaymentMetadata = &intent.SendPublicMultiPaymentMetadata{
			Source:   obj.Source,
			Quantity: obj.Quantity,

			ExchangeCurrency: currency.Code(obj.ExchangeCurrency),
			ExchangeRate:     obj.ExchangeRate,
			NativeAmount:     obj.NativeAmount,
			UsdMarketValue:   obj.UsdMarketValue,
		}

		for _, account := range obj.Accounts {
			record.SendPublicMultiPaymentMetadata.Payments = append(record.SendPublicMultiPaymentMetadata.Payments, toPayment(account))
		}
	}

	return record
}

func (m *intentModel) dbSave(ctx context.Context, db *sqlx.DB) error {
	canInsertAccounts := m.Id.Int64 == 0 && len(m.Accounts) > 0

	return pgutil.ExecuteInTx(ctx, db, sql.LevelDefault, func(tx *sqlx.Tx) error {
		query := `INSERT INTO ` + intentTableName + `
			(intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, original_usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, app_metadata, state, version, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21 + 1, $22)

			ON CONFLICT (intent_id)
			DO UPDATE
				SET usd_market_value = $12, state = $20, version = ` + intentTableName + `.version + 1
				WHERE ` + intentTableName + `.intent_id = $1 AND ` + intentTableName + `.version = $21

			RETURNING
				id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, original_usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, app_metadata, state, version, created_at`

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
			m.OriginalUsdMarketValue,
			m.IsWithdrawal,
			m.IsIndirectSend,
			m.IsReturned,
			m.IsIssuerVoidingGiftCard,
			m.IsSwap,
			m.AppMetadata,
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

	NativeAmount   sql.NullFloat64 `db:"native_amount"`
	UsdMarketValue sql.NullFloat64 `db:"usd_market_value"`
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

func fromPayment(pagingID int64, obj *intent.Payment) *intentAccountModel {
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

		NativeAmount: sql.NullFloat64{
			Valid:   true,
			Float64: obj.NativeAmount,
		},
		UsdMarketValue: sql.NullFloat64{
			Valid:   true,
			Float64: obj.UsdMarketValue,
		},
	}
}

func toPayment(obj *intentAccountModel) *intent.Payment {
	return &intent.Payment{
		DestinationOwnerAccount: obj.DestinationOwner.String,
		DestinationTokenAccount: obj.Destination.String,
		Quantity:                obj.Quantity,

		NativeAmount:   obj.NativeAmount.Float64,
		UsdMarketValue: obj.UsdMarketValue.Float64,
	}
}

func dbBatchPutAccounts(ctx context.Context, tx *sqlx.Tx, models []*intentAccountModel) ([]*intentAccountModel, error) {
	var res []*intentAccountModel

	query := `INSERT INTO ` + accountsTableName + `
			(paging_id, source, source_owner, destination, destination_owner, quantity, native_amount, usd_market_value)
			VALUES `
	var parameters []any

	for i, m := range models {
		baseIndex := len(parameters)
		query += fmt.Sprintf(
			`($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)`,
			baseIndex+1, baseIndex+2, baseIndex+3, baseIndex+4, baseIndex+5, baseIndex+6, baseIndex+7, baseIndex+8,
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
			m.NativeAmount,
			m.UsdMarketValue,
		)
	}
	query += ` RETURNING id, paging_id, source, source_owner, destination, destination_owner, quantity, native_amount, usd_market_value`

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
	if intentType != intent.PublicDistribution && intentType != intent.SendPublicMultiPayment {
		return res, nil
	}

	query := `SELECT id, paging_id, source, source_owner, destination, destination_owner, quantity, native_amount, usd_market_value
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

	query := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, original_usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, app_metadata, state, version, created_at
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

	query := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, original_usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, app_metadata, state, version, created_at
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
	query1 := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, original_usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, app_metadata, state, version, created_at
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

	query := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, original_usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, app_metadata, state, version, created_at
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

	query := `SELECT id, intent_id, intent_type, mint, owner, source, destination_owner, destination, quantity, exchange_currency, exchange_rate, native_amount, usd_market_value, original_usd_market_value, is_withdraw, is_remote_send, is_returned, is_issuer_voiding_gift_card, is_swap, app_metadata, state, version, created_at
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
		WHERE owner = $1 AND created_at >= $2 AND intent_type IN ($3, $5) AND state != $4 AND is_withdraw = FALSE
	`
	err := db.GetContext(
		ctx,
		&res,
		query,
		owner,
		since,
		intent.SendPublicPayment,
		intent.StateRevoked,
		intent.SendPublicMultiPayment,
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
		mintFilter = "i.mint IS NULL"
		params = []any{owner, intent.StateRevoked, intent.ExternalDeposit, intent.ReceivePaymentsPublicly, intent.SendPublicPayment, intent.SendPublicMultiPayment}
	} else {
		mintFilter = "i.mint = $7"
		params = []any{owner, intent.StateRevoked, intent.ExternalDeposit, intent.ReceivePaymentsPublicly, intent.SendPublicPayment, intent.SendPublicMultiPayment, mint}
	}

	// USD received as destination:
	//   - ExternalDeposit, ReceivePaymentsPublicly where owner is initiator
	//   - SendPublicPayment where owner is destination_owner
	//   - SendPublicMultiPayment where owner is a payment's destination_owner
	// USD sent as source:
	//   - SendPublicPayment, SendPublicMultiPayment where owner is initiator
	query := fmt.Sprintf(`SELECT
		(SELECT COALESCE(SUM(i.usd_market_value), 0) FROM %s i WHERE i.owner = $1 AND %s AND i.intent_type IN ($3, $4) AND i.state != $2) +
		(SELECT COALESCE(SUM(i.usd_market_value), 0) FROM %s i WHERE i.destination_owner = $1 AND %s AND i.intent_type = $5 AND i.state != $2) +
		(SELECT COALESCE(SUM(a.usd_market_value), 0) FROM %s a JOIN %s i ON i.id = a.paging_id WHERE a.destination_owner = $1 AND %s AND i.intent_type = $6 AND i.state != $2) -
		(SELECT COALESCE(SUM(i.usd_market_value), 0) FROM %s i WHERE i.owner = $1 AND %s AND i.intent_type IN ($5, $6) AND i.state != $2);`,
		intentTableName, mintFilter,
		intentTableName, mintFilter,
		accountsTableName, intentTableName, mintFilter,
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

func dbGetUsdCostBasisBatch(ctx context.Context, db *sqlx.DB, mint string, owners ...string) (map[string]float64, error) {
	if len(owners) == 0 {
		return make(map[string]float64), nil
	}

	type Row struct {
		Owner     string  `db:"owner"`
		CostBasis float64 `db:"cost_basis"`
	}
	rows := []*Row{}

	individualFilters := make([]string, len(owners))
	for i, owner := range owners {
		individualFilters[i] = fmt.Sprintf("'%s'", owner)
	}
	ownerFilter := strings.Join(individualFilters, ",")

	// For backwards compatibility, the mint column is NULL for core mint
	var mintFilter string
	var params []any
	if mint == config.CoreMintPublicKeyString {
		mintFilter = "i.mint IS NULL"
		params = []any{intent.StateRevoked, intent.ExternalDeposit, intent.ReceivePaymentsPublicly, intent.SendPublicPayment, intent.SendPublicMultiPayment}
	} else {
		mintFilter = "i.mint = $6"
		params = []any{intent.StateRevoked, intent.ExternalDeposit, intent.ReceivePaymentsPublicly, intent.SendPublicPayment, intent.SendPublicMultiPayment, mint}
	}

	// USD received as destination (owner is initiator for ExternalDeposit, ReceivePaymentsPublicly):
	// USD received as destination (owner is destination_owner for SendPublicPayment):
	// USD received as destination (owner is a payment's destination_owner for SendPublicMultiPayment):
	// USD sent as source (owner is initiator for SendPublicPayment, SendPublicMultiPayment):
	query := fmt.Sprintf(
		`(SELECT i.owner AS owner, COALESCE(SUM(i.usd_market_value), 0) AS cost_basis FROM %s i WHERE i.owner IN (%s) AND %s AND i.intent_type IN ($2, $3) AND i.state != $1 GROUP BY i.owner)
		UNION ALL
		(SELECT i.destination_owner AS owner, COALESCE(SUM(i.usd_market_value), 0) AS cost_basis FROM %s i WHERE i.destination_owner IN (%s) AND %s AND i.intent_type = $4 AND i.state != $1 GROUP BY i.destination_owner)
		UNION ALL
		(SELECT a.destination_owner AS owner, COALESCE(SUM(a.usd_market_value), 0) AS cost_basis FROM %s a JOIN %s i ON i.id = a.paging_id WHERE a.destination_owner IN (%s) AND %s AND i.intent_type = $5 AND i.state != $1 GROUP BY a.destination_owner)
		UNION ALL
		(SELECT i.owner AS owner, -COALESCE(SUM(i.usd_market_value), 0) AS cost_basis FROM %s i WHERE i.owner IN (%s) AND %s AND i.intent_type IN ($4, $5) AND i.state != $1 GROUP BY i.owner);`,
		intentTableName, ownerFilter, mintFilter,
		intentTableName, ownerFilter, mintFilter,
		accountsTableName, intentTableName, ownerFilter, mintFilter,
		intentTableName, ownerFilter, mintFilter,
	)

	err := db.SelectContext(ctx, &rows, query, params...)
	if err != nil {
		return nil, err
	}

	res := make(map[string]float64)
	for _, owner := range owners {
		res[owner] = 0
	}
	for _, row := range rows {
		res[row.Owner] += row.CostBasis
	}
	return res, nil
}
