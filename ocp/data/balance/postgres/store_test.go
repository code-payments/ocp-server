package postgres

import (
	"database/sql"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/data/balance"
	"github.com/code-payments/ocp-server/ocp/data/balance/tests"

	postgrestest "github.com/code-payments/ocp-server/database/postgres/test"

	_ "github.com/jackc/pgx/v4/stdlib"
)

var (
	testStore balance.Store
	teardown  func()
)

const (
	// Used for testing ONLY, the table and migrations are external to this repository
	tableCreate = `
	CREATE TABLE ocp__core_cachedbalanceversion (
		id SERIAL NOT NULL PRIMARY KEY,

		token_account TEXT NOT NULL,
		version INTEGER NOT NULL,

		CONSTRAINT ocp__core_cachedbalanceversion__unique__token_account UNIQUE (token_account)
	);

	CREATE TABLE ocp__core_opencloselocks (
		id SERIAL NOT NULL PRIMARY KEY,

		token_account TEXT NOT NULL,
		is_open BOOL NOT NULL,

		CONSTRAINT ocp__core_opencloselocks__unique__token_account UNIQUE (token_account)
	);

	CREATE TABLE ocp__core_externalbalancecheckpoint (
		id SERIAL NOT NULL PRIMARY KEY,

		token_account TEXT NOT NULL,
		quarks INTEGER NOT NULL,
		slot_checkpoint INTEGER NOT NULL,

		last_updated_at TIMESTAMP WITH TIME ZONE,

		CONSTRAINT ocp__core_balanceexternalcheckpoint__uniq__token_account UNIQUE (token_account)
	);
	`

	// Used for testing ONLY, the table and migrations are external to this repository
	tableDestroy = `
		DROP TABLE ocp__core_cachedbalanceversion;
		DROP TABLE ocp__core_opencloselocks;
		DROP TABLE ocp__core_externalbalancecheckpoint;
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

func TestBalancePostgresStore(t *testing.T) {
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
