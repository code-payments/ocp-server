package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/ocp-server/ocp/data/task"
	"github.com/code-payments/ocp-server/ocp/data/task/tests"

	pgutil "github.com/code-payments/ocp-server/database/postgres"
	postgrestest "github.com/code-payments/ocp-server/database/postgres/test"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	// Used for testing ONLY, the table and migrations are external to this repository
	tableCreate = `
		CREATE TABLE ocp__core_task(
			id SERIAL NOT NULL PRIMARY KEY,

			task_id TEXT NOT NULL UNIQUE,

			task_type INTEGER NOT NULL,
			data BYTEA,

			reference_id TEXT,

			state INTEGER NOT NULL,

			failed_attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL,

			version INTEGER NOT NULL,

			created_at TIMESTAMP WITH TIME ZONE NOT NULL
		);

		CREATE INDEX ocp__core_task_ready_by_state_idx ON ocp__core_task (state, next_attempt_at, id);
	`

	// Used for testing ONLY, the table and migrations are external to this repository
	tableDestroy = `
		DROP TABLE ocp__core_task;
	`
)

var (
	testStore task.Store
	testDb    *sqlx.DB
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
	testDb = sqlx.NewDb(db, "pgx")
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

func TestTaskPostgresStore(t *testing.T) {
	tests.RunTests(t, testStore, teardown)
}

// Tasks must be created atomically with whatever DB transaction is being
// passed along the context (eg. the one that commits an intent).
func TestTaskPostgresStoreTxSupport(t *testing.T) {
	defer teardown()

	ctx := context.Background()

	record := &task.Record{
		TaskId:        "test_task_id",
		Type:          1,
		Data:          []byte("test_data"),
		State:         task.StatePending,
		NextAttemptAt: time.Now(),
		CreatedAt:     time.Now(),
	}

	errRollback := errors.New("rollback")
	err := pgutil.ExecuteTxWithinCtx(ctx, testDb, sql.LevelDefault, func(ctx context.Context) error {
		if err := testStore.PutAll(ctx, record); err != nil {
			return err
		}
		return errRollback
	})
	assert.Equal(t, errRollback, err)

	_, err = testStore.GetByTaskId(ctx, "test_task_id")
	assert.Equal(t, task.ErrNotFound, err)

	record.Id = 0
	record.Version = 0
	err = pgutil.ExecuteTxWithinCtx(ctx, testDb, sql.LevelDefault, func(ctx context.Context) error {
		if err := testStore.PutAll(ctx, record); err != nil {
			return err
		}

		// The task is visible within the transaction via the same connection
		return testStore.Update(ctx, record)
	})
	require.NoError(t, err)

	actual, err := testStore.GetByTaskId(ctx, "test_task_id")
	require.NoError(t, err)
	assert.EqualValues(t, 2, actual.Version)
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
