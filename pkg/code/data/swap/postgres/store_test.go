package postgres

import (
	"database/sql"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/pkg/code/data/swap"
	"github.com/code-payments/ocp-server/pkg/code/data/swap/tests"

	postgrestest "github.com/code-payments/ocp-server/pkg/database/postgres/test"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	// Used for testing ONLY, the table and migrations are external to this repository
	tableCreate = `
		CREATE TABLE ocp__core_swap(
			id SERIAL NOT NULL PRIMARY KEY,

			swap_id TEXT NOT NULL UNIQUE,

			owner TEXT NOT NULL,

			from_mint TEXT NOT NULL,
			to_mint TEXT NOT NULL,
			amount BIGINT NULL CHECK (amount > 0),

			funding_id TEXT NOT NULL UNIQUE,
			funding_source INTEGER NOT NULL,

			nonce TEXT NOT NULL,
			blockhash TEXT NOT NULL,

			proof_signature TEXT NOT NULL,

			transaction_signature TEXT UNIQUE,
			transaction_blob BYTEA,

			state INTEGER NOT NULL,
			version INTEGER NOT NULL,

			created_at TIMESTAMP WITH TIME ZONE NOT NULL
		)
	`

	// Used for testing ONLY, the table and migrations are external to this repository
	tableDestroy = `
		DROP TABLE ocp__core_swap;
	`
)

var (
	testStore swap.Store
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

func TestSwapPostgresStore(t *testing.T) {
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
