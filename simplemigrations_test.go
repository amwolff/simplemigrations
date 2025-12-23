package simplemigrations

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/amwolff/simplemigrations/internal/fruitsdbx"

	"github.com/zeebo/assert"
)

func TestMigrateToLatestWithSchema(t *testing.T) {
	t.Parallel()

	retryFunc := func() func(err error) bool {
		var retries int
		return func(err error) bool {
			// NOTE: you usually would check for specific errors here,
			// like 42P01 (undefined table) in Postgres.
			if retries > 3 {
				return false
			}
			retries++
			return true
		}
	}

	t.Run("postgres", func(t *testing.T) {
		t.Parallel()

		const schema = "ADC83B19"

		ctx := context.TODO()
		log := &testLogger{t: t}
		adapter := newPostgresAdapter(t)

		_, err := MigrateToLatestWithSchema(ctx, log, adapter, "", true, versions, retryFunc()) // schema cannot be empty
		assert.Error(t, err)

		_, err = MigrateToLatestWithSchema(ctx, log, adapter, schema, false, versionsInRandomOrder, retryFunc())
		assert.Error(t, err)
		_, err = MigrateToLatestWithSchema(ctx, log, adapter, schema, false, versionsWithDuplicate, retryFunc())
		assert.Error(t, err)

		cleanup, err := MigrateToLatestWithSchema(ctx, log, adapter, schema, true, versions, retryFunc())
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, cleanup()) })
		cleanup, err = MigrateToLatestWithSchema(ctx, log, adapter, schema, true, versions, retryFunc())
		assert.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, cleanup())
			assert.NoError(t, cleanup()) // ensure idempotency
		})
		cleanup, err = MigrateToLatestWithSchema(ctx, log, adapter, schema, true, versions, retryFunc()) // ensure idempotency
		assert.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, cleanup())
			assert.NoError(t, cleanup())
			assert.NoError(t, cleanup()) // ensure idempotency
		})

		tx, err := adapter.db.Open(ctx)
		assert.NoError(t, err)
		adapted := newPostgresTxAdapter(tx)
		t.Cleanup(func() { assert.NoError(t, RollbackUnlessCommitted(ctx, log, adapted)) })
		assert.NoError(t, SetSearchPathTo(ctx, adapted, schema))

		no, err := adapted.LatestSchemaVersion(ctx)
		assert.NoError(t, err)
		assert.Equal(t, versions[len(versions)-1].Version, no)
	})
	t.Run("postgres, ensure cleanup is never nil", func(t *testing.T) {
		t.Parallel()

		const schema = "is it?"

		ctx := context.TODO()
		log := &testLogger{t: t}
		adapter := newPostgresAdapter(t)

		cleanup, err := MigrateToLatestWithSchema(ctx, log, adapter, schema, false, versions, retryFunc())
		assert.NoError(t, err)
		assert.NotNil(t, cleanup)
		assert.NoError(t, cleanup())

		cleanup, err = MigrateToLatestWithSchema(ctx, log, adapter, schema, true, versions, retryFunc())
		assert.NoError(t, err)
		assert.NoError(t, cleanup())
	})
	t.Run("custom", func(t *testing.T) {
		t.Parallel()

		ctx := context.TODO()
		log := &testLogger{t: t}
		adapter := &customAdapter{}

		_, err := MigrateToLatestWithSchema(ctx, log, adapter, "schema", true, versions, retryFunc())
		assert.Error(t, err)
	})
}

func TestMigrateToLatest(t *testing.T) {
	t.Parallel()

	t.Run("postgres", func(t *testing.T) {
		t.Parallel()

		ctx := context.TODO()
		log := &testLogger{t: t}
		adapter := newPostgresAdapter(t)

		testMigrateToLatest(ctx, t, adapter, log)
	})
	t.Run("custom", func(t *testing.T) {
		t.Parallel()

		ctx := context.TODO()
		log := &testLogger{t: t}
		adapter := &customAdapter{}

		testMigrateToLatest(ctx, t, adapter, log)
	})
}

func testMigrateToLatest(ctx context.Context, t *testing.T, db DB, log Logger) {
	tx, err := db.Open(ctx)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, RollbackUnlessCommitted(ctx, log, tx)) })

	assert.Error(t, MigrateToLatest(ctx, log, tx, versionsInRandomOrder, true))
	assert.Error(t, MigrateToLatest(ctx, log, tx, versionsWithDuplicate, true))

	assert.NoError(t, MigrateToLatest(ctx, log, tx, versions, true))
	assert.NoError(t, MigrateToLatest(ctx, log, tx, versions, false))
	assert.NoError(t, MigrateToLatest(ctx, log, tx, versions, false)) // ensure idempotency

	assert.Error(t, MigrateToLatest(ctx, log, tx, versions[:1], false)) // cannot migrate to lower version

	no, err := tx.LatestSchemaVersion(ctx)
	assert.NoError(t, err)
	assert.Equal(t, versions[len(versions)-1].Version, no)
}

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debug(_ context.Context, msg string, args ...any) {
	l.t.Logf("[DEBUG]: msg: %s, args: %v", msg, args)
}

func (l *testLogger) Info(_ context.Context, msg string, args ...any) {
	l.t.Logf("[INFO]: msg: %s, args: %v", msg, args)
}

func (l *testLogger) Warn(_ context.Context, msg string, args ...any) {
	l.t.Logf("[WARN]: msg: %s, args: %v", msg, args)
}

type postgresAdapter struct {
	db *fruitsdbx.DB
}

func (a *postgresAdapter) Dialect() Dialect {
	return DialectPostgres
}

func (a *postgresAdapter) Open(ctx context.Context) (Tx, error) {
	tx, err := a.db.Open(ctx)
	return &postgresTxAdapter{tx: tx}, err
}

func (a *postgresAdapter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := a.db.ExecContext(ctx, query, args...)
	return err
}

type postgresTxAdapter struct {
	tx *fruitsdbx.Tx
}

func (a *postgresTxAdapter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := a.tx.ExecContext(ctx, query, args...)
	return err
}

func (a *postgresTxAdapter) LatestSchemaVersion(ctx context.Context) (int, error) {
	row, err := a.tx.First_SchemaVersion_Number_OrderBy_Desc_Number(ctx)
	if err != nil {
		return 0, err
	}
	return row.Number, nil
}

func (a *postgresTxAdapter) CreateSchema(ctx context.Context, version int, comment string) error {
	_, err := a.tx.Create_SchemaVersion(ctx, fruitsdbx.SchemaVersion_Number(version), fruitsdbx.SchemaVersion_Comment(comment))
	return err
}

func (a *postgresTxAdapter) Commit() error {
	return a.tx.Commit()
}

func (a *postgresTxAdapter) Rollback() error {
	return a.tx.Rollback()
}

func newPostgresAdapter(t *testing.T) *postgresAdapter {
	source, ok := os.LookupEnv("AMWOLFF_SIMPLEMIGRATIONS_TEST_POSTGRES")
	if !ok {
		t.Skip("AMWOLFF_SIMPLEMIGRATIONS_TEST_POSTGRES is not set")
	}

	db, err := fruitsdbx.Open("pgx", source)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	return &postgresAdapter{db: db}
}

func newPostgresTxAdapter(tx *fruitsdbx.Tx) *postgresTxAdapter {
	return &postgresTxAdapter{tx: tx}
}

type customAdapter struct {
	versions int
}

func (a *customAdapter) Dialect() Dialect {
	return "custom"
}

func (a *customAdapter) Open(context.Context) (Tx, error) {
	return &customTxAdapter{db: a}, nil
}

func (a *customAdapter) ExecContext(context.Context, string, ...any) error {
	return nil
}

type customTxAdapter struct {
	db       *customAdapter
	versions int
	done     bool
}

func (a *customTxAdapter) ExecContext(context.Context, string, ...any) error {
	return nil
}

func (a *customTxAdapter) LatestSchemaVersion(context.Context) (int, error) {
	return max(a.db.versions, a.versions), nil
}

func (a *customTxAdapter) CreateSchema(_ context.Context, version int, _ string) error {
	if version < a.db.versions || version < a.versions {
		return errors.New("cannot create schema with lower version")
	}
	a.versions = version
	return nil
}

func (a *customTxAdapter) Commit() error {
	if a.done {
		return sql.ErrTxDone
	}
	a.db.versions = a.versions
	a.done = true
	return nil
}

func (a *customTxAdapter) Rollback() error {
	if a.done {
		return sql.ErrTxDone
	}
	a.done = true
	return nil
}

var versions = []Migration{
	{
		Queries: []string{
			`CREATE TABLE schema_versions (
	number integer NOT NULL,
	comment text NOT NULL,
	PRIMARY KEY ( number )
)`,
		},
		Version:        1,
		VersionComment: "initial version (schema_versions table)",
	},
	{
		Queries: []string{
			`CREATE TABLE fruits (
	id integer NOT NULL,
	name varchar(50) NOT NULL,
	color varchar(20) NOT NULL,
	calories_per_100g integer NOT NULL,
	is_tropical boolean NOT NULL,
	PRIMARY KEY ( id )
)`,
		},
		Version:        2,
		VersionComment: "create fruits table",
	},
	{
		Queries: []string{
			"INSERT INTO fruits VALUES (1,'Apple','Red',52,FALSE)",
			"INSERT INTO fruits VALUES (2,'Banana','Yellow',89,TRUE)",
			"INSERT INTO fruits VALUES (3,'Orange','Orange',47,FALSE)",
			"INSERT INTO fruits VALUES (4,'Strawberry','Red',32,FALSE)",
			"INSERT INTO fruits VALUES (5,'Mango','Orange',60,TRUE)",
			"INSERT INTO fruits VALUES (6,'Pineapple','Yellow',50,TRUE)",
			"INSERT INTO fruits VALUES (7,'Grapes','Purple',69,FALSE)",
			"INSERT INTO fruits VALUES (8,'Watermelon','Green',30,TRUE)",
			"INSERT INTO fruits VALUES (9,'Kiwi','Brown',61,FALSE)",
			"INSERT INTO fruits VALUES (10,'Avocado','Green',160,TRUE)",
			"INSERT INTO fruits VALUES (11,'Blueberry','Blue',57,FALSE)",
			"INSERT INTO fruits VALUES (12,'Peach','Orange',39,FALSE)",
		},
		Version:        3,
		VersionComment: "insert fruits into the fruits table",
	},
}

var (
	versionsInRandomOrder = []Migration{
		versions[0],
		versions[2],
		versions[1],
	}
	versionsWithDuplicate = []Migration{
		versions[0],
		versions[1],
		versions[1],
		versions[2],
	}
)
