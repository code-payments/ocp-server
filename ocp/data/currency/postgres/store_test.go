package postgres

import (
	"database/sql"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/tests"

	postgrestest "github.com/code-payments/ocp-server/database/postgres/test"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	// Used for testing ONLY, the table and migrations are external to this repository
	tableCreate = `
	CREATE TABLE ocp__core_exchangerate (
		id serial NOT NULL PRIMARY KEY, 

		for_date VARCHAR(10) NOT NULL, 
		for_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, 
		currency_code VARCHAR(3) NOT NULL, 
		currency_rate NUMERIC(18, 9) NOT NULL,

		CONSTRAINT ocp__core_exchangerate__uniq__timestamp__and__code UNIQUE (for_timestamp, currency_code),
		CONSTRAINT ocp__core_exchangerate__currency_code CHECK (currency_code::text ~ '^[a-z]{3}$')
	);
	CREATE TABLE ocp__core_currencymetadata (
		id serial NOT NULL PRIMARY KEY,

		name TEXT NOT NULL,
		symbol TEXT NOT NULL,
		description TEXT NOT NULL,
		image_url TEXT NOT NULL,

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

		created_by TEXT NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL
	);
	CREATE TABLE ocp__core_currencyreserve (
		id serial NOT NULL PRIMARY KEY,

		for_date VARCHAR(10) NOT NULL, 
		for_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, 
		mint TEXT NOT NULL, 
		supply_from_bonding BIGINT NOT NULL,

		CONSTRAINT ocp__core_currencyreserve__uniq__timestamp__and__mint UNIQUE (for_timestamp, mint)
	);
	`

	// Used for testing ONLY, the table and migrations are external to this repository
	tableDestroy = `
		DROP TABLE ocp__core_exchangerate;
		DROP TABLE ocp__core_currencymetadata;
		DROP TABLE ocp__core_currencyreserve;
	`
)

var (
	testStore currency.Store
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

func TestCurrencyPostgresStore(t *testing.T) {
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
