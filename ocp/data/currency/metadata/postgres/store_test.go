package postgres

import (
	"database/sql"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/data/currency/metadata"
	"github.com/code-payments/ocp-server/ocp/data/currency/metadata/tests"

	postgrestest "github.com/code-payments/ocp-server/database/postgres/test"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	// Used for testing ONLY, the table and migrations are external to this repository
	tableCreate = `
	CREATE TABLE ocp__core_currencymetadata (
		id serial NOT NULL PRIMARY KEY,

		name TEXT NOT NULL,
		symbol TEXT NOT NULL,
		description TEXT NOT NULL,
		image_url TEXT NOT NULL,
		bill_colors TEXT NOT NULL DEFAULT '',
		social_links TEXT NOT NULL DEFAULT '[]',

		seed TEXT UNIQUE NOT NULL,

		authority TEXT NOT NULL,

		mint TEXT UNIQUE NOT NULL,
		mint_bump INTEGER NOT NULL,
		decimals INTEGER NOT NULL,

		currency_config TEXT UNIQUE NOT NULL,
		currency_config_bump INTEGER NOT NULL,

		liquidity_pool TEXT UNIQUE NOT NULL,
		liquidity_pool_bump INTEGER NOT NULL,

		vault_mint TEXT UNIQUE NOT NULL,
		vault_mint_bump INTEGER NOT NULL,

		vault_core TEXT UNIQUE NOT NULL,
		vault_core_bump INTEGER NOT NULL,

		sell_fee_bps INTEGER NOT NULL,

		alt TEXT NOT NULL,

		state INTEGER NOT NULL,
		version BIGINT NOT NULL,

		created_by TEXT NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL
	);
	CREATE UNIQUE INDEX ocp__core_currencymetadata__name__idx ON ocp__core_currencymetadata (LOWER(name)) WHERE state != 8;
	`

	// Used for testing ONLY, the table and migrations are external to this repository
	tableDestroy = `
		DROP TABLE ocp__core_currencymetadata;
	`
)

var (
	testStore metadata.Store
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

func TestMetadata_PostgresStore(t *testing.T) {
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
