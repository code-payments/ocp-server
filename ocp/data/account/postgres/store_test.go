package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	commonpb "github.com/code-payments/ocp-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/ocp-server/ocp/data/account"
	"github.com/code-payments/ocp-server/ocp/data/account/tests"

	postgrestest "github.com/code-payments/ocp-server/database/postgres/test"

	_ "github.com/jackc/pgx/v4/stdlib"
)

var (
	testStore account.Store
	testDB    *sql.DB
	teardown  func()
)

const (
	// Used for testing ONLY, the table and migrations are external to this repository
	tableCreate = `
	CREATE TABLE ocp__core_accountinfo (
		id SERIAL NOT NULL PRIMARY KEY,

		owner_account TEXT NOT NULL,
		authority_account TEXT NOT NULL,
		token_account TEXT NOT NULL,
		mint_account TEXT NOT NULL,

		account_type INTEGER NOT NULL,
		index INTEGER NOT NULL,

		requires_deposit_sync BOOL NOT NULL,
		deposits_last_synced_at TIMESTAMP WITH TIME ZONE NOT NULL,

		requires_auto_return_check BOOL NOT NULL,

		balance BIGINT,

		created_at TIMESTAMP WITH TIME ZONE NOT NULL,

		CONSTRAINT ocp__core_accountinfov2__uniq__token_account UNIQUE (token_account),
		CONSTRAINT ocp__core_accountinfov2__uniq__authority_account__and__mint_account UNIQUE (authority_account, mint_account),
		CONSTRAINT ocp__core_accountinfov2__uniq__owner_account__and__mint_account__and__account_type__and__index UNIQUE(owner_account, mint_account, account_type, index),
		CONSTRAINT ocp__core_accountinfov2__check__balance_nonnegative CHECK (balance IS NULL OR balance >= 0)
	);
	`

	// Used for testing ONLY, the table and migrations are external to this repository
	tableDestroy = `
		DROP TABLE ocp__core_accountinfo;
	`
)

func TestMain(m *testing.M) {
	log := zap.Must(zap.NewDevelopment())

	testPool, err := dockertest.NewPool("")
	if err != nil {
		log.With(zap.Error(err)).Error("Error creating docker pool")
		os.Exit(1)
	}

	var cleanUpFunc func()
	db, cleanUpFunc, err := postgrestest.StartPostgresDB(testPool)
	if err != nil {
		log.With(zap.Error(err)).Error("Error starting postgres image")
		os.Exit(1)
	}
	defer db.Close()

	if err := createTestTables(log, db); err != nil {
		log.With(zap.Error(err)).Error("Error creating test tables")
		cleanUpFunc()
		os.Exit(1)
	}

	testStore = New(db)
	testDB = db
	teardown = func() {
		if pc := recover(); pc != nil {
			cleanUpFunc()
			panic(pc)
		}

		if err := resetTestTables(log, db); err != nil {
			log.With(zap.Error(err)).Error("Error resetting test tables")
			cleanUpFunc()
			os.Exit(1)
		}
	}

	code := m.Run()
	cleanUpFunc()
	os.Exit(code)
}

func TestAccountPostgresStore(t *testing.T) {
	tests.RunTests(t, testStore, teardown)
}

func TestLegacyUninitializedBalance(t *testing.T) {
	ctx := context.Background()

	record := &account.Record{
		OwnerAccount:     "legacy_owner",
		AuthorityAccount: "legacy_owner",
		TokenAccount:     "legacy_token",
		MintAccount:      "mint",
		AccountType:      commonpb.AccountType_PRIMARY,
	}
	require.NoError(t, testStore.Put(ctx, record))

	// Simulate a legacy pre-migration row, created before the balance column
	// existed, by clearing its stored balance.
	_, err := testDB.Exec("UPDATE ocp__core_accountinfo SET balance = NULL WHERE token_account = $1", record.TokenAccount)
	require.NoError(t, err)

	tests.RunUninitializedBalanceTests(t, testStore, record.TokenAccount)
}

func createTestTables(log *zap.Logger, db *sql.DB) error {
	_, err := db.Exec(tableCreate)
	if err != nil {
		log.With(zap.Error(err)).Error("could not create test tables")
		return err
	}
	return nil
}

func resetTestTables(log *zap.Logger, db *sql.DB) error {
	_, err := db.Exec(tableDestroy)
	if err != nil {
		log.With(zap.Error(err)).Error("could not drop test tables")
		return err
	}

	return createTestTables(log, db)
}
