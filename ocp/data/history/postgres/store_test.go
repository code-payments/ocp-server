package postgres

import (
	"database/sql"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/data/history"
	"github.com/code-payments/ocp-server/ocp/data/history/tests"

	postgrestest "github.com/code-payments/ocp-server/database/postgres/test"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	// Used for testing ONLY, the table and migrations are external to this repository
	tableCreate = `
		CREATE TABLE ocp__core_transactionhistory(
			id SERIAL NOT NULL PRIMARY KEY,

			reference_id TEXT NOT NULL,
			reference_type INTEGER NOT NULL,

			type INTEGER NOT NULL,

			owner_account TEXT NOT NULL,
			counterparty_owner_account TEXT NULL,

			exchange_currency TEXT NOT NULL,
			native_amount DOUBLE PRECISION NOT NULL,

			fees TEXT NOT NULL DEFAULT '[]',

			mint_account TEXT NOT NULL,
			quantity BIGINT NOT NULL CHECK (quantity > 0),

			destination_mint_account TEXT NULL,
			destination_quantity BIGINT NULL CHECK (destination_quantity > 0),

			gift_card_vault TEXT NULL,
			app_metadata BYTEA NULL,

			version INTEGER NOT NULL,

			state INTEGER NOT NULL,

			created_at TIMESTAMP WITH TIME ZONE NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

			CONSTRAINT ocp__core_transactionhistory__uniq__owner__and__reference UNIQUE (owner_account, reference_type, reference_id)
		);

		CREATE INDEX ocp__core_transactionhistory__idx__owner__and__time ON ocp__core_transactionhistory(owner_account, created_at, id);
		CREATE INDEX ocp__core_transactionhistory__idx__owner__and__mint ON ocp__core_transactionhistory(owner_account, mint_account, created_at, id);
		CREATE INDEX ocp__core_transactionhistory__idx__owner__and__destmint ON ocp__core_transactionhistory(owner_account, destination_mint_account, created_at, id) WHERE destination_mint_account IS NOT NULL;
		CREATE INDEX ocp__core_transactionhistory__idx__reference ON ocp__core_transactionhistory(reference_type, reference_id);
		CREATE INDEX ocp__core_transactionhistory__idx__giftcardvault ON ocp__core_transactionhistory(gift_card_vault) WHERE gift_card_vault IS NOT NULL;
	`

	// Used for testing ONLY, the table and migrations are external to this repository
	tableDestroy = `
		DROP TABLE ocp__core_transactionhistory;
	`
)

var (
	testStore history.Store
	teardown  func()
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

func TestHistoryPostgresStore(t *testing.T) {
	tests.RunTests(t, testStore, teardown)
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
