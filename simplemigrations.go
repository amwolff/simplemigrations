// Package simplemigrations provides helpers for applying ordered
// migrations against transactional backends.
package simplemigrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// Dialect identifies the database dialect supported by
// simplemigrations.
type Dialect string

const (
	// DialectPostgres identifies the PostgreSQL dialect.
	DialectPostgres Dialect = "postgres"
)

// Logger records diagnostic messages about migration progress.
type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
}

type (
	// DB opens transactional connections and reports the active
	// dialect.
	DB interface {
		Dialect() Dialect
		Open(ctx context.Context) (Tx, error)
		ExecContext(ctx context.Context, query string, args ...any) error
	}
	// Tx wraps MinimalTx with commit and rollback semantics.
	Tx interface {
		MinimalTx
		Commit() error
		Rollback() error
	}
	// MinimalTx executes migration queries and manages schema versions.
	MinimalTx interface {
		ExecContext(ctx context.Context, query string, args ...any) error
		LatestSchemaVersion(ctx context.Context) (int, error)
		CreateSchema(ctx context.Context, version int, comment string) error
	}
)

// NopLogger implements Logger without producing any output.
type NopLogger struct{}

// Debug discards debug messages.
func (NopLogger) Debug(context.Context, string, ...any) {}

// Info discards informational messages.
func (NopLogger) Info(context.Context, string, ...any) {}

// Warn discards warning messages.
func (NopLogger) Warn(context.Context, string, ...any) {}

// Migration describes a single schema change and its metadata.
type Migration struct {
	Queries        []string
	Version        int
	VersionComment string
}

func validateMigrations(migrations []Migration) error {
	if !slices.IsSortedFunc(migrations, func(a, b Migration) int {
		return a.Version - b.Version
	}) {
		return errors.New("migrations are not sorted")
	}

	for i := 1; i < len(migrations); i++ {
		if migrations[i-1].Version == migrations[i].Version {
			return fmt.Errorf("duplicate version %d at %d, %d", migrations[i].Version, i-1, i)
		}
	}

	return nil
}

// MigrateToLatest runs pending migrations within the provided
// transaction.
func MigrateToLatest(ctx context.Context, log Logger, tx MinimalTx, migrations []Migration, freshDB bool) error {
	if err := validateMigrations(migrations); err != nil {
		return err
	}
	return migrateToLatest(ctx, log, tx, migrations, freshDB)
}

// MigrateToLatestWithSchema runs pending migrations, optionally
// creating an isolated schema.
func MigrateToLatestWithSchema(ctx context.Context, log Logger, db DB, schema string, temporary bool, migrations []Migration) (cleanup func() error, err error) {
	if db.Dialect() != DialectPostgres {
		return nil, errors.New("only postgres dialect is supported")
	}
	if schema == "" && temporary {
		return nil, errors.New("the temporary option requires a schema name")
	}
	if err = validateMigrations(migrations); err != nil {
		return nil, err
	}

	var freshDB bool
Retry:
	tx, err := db.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, RollbackUnlessCommitted(ctx, log, tx))
	}()

	if schema != "" {
		if cleanup, err = createSchema(ctx, db, tx, schema, temporary); err != nil {
			return nil, err
		}
	}

	if err = migrateToLatest(ctx, log, tx, migrations, freshDB); err != nil {
		// TODO(amwolff): this is bad for several reasons, although I
		// think it checks the box for now; what I believe we should do
		// here instead is for both MigrateToLatest… handlers to accept
		// a function that will determine whether the error is
		// retriable.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			freshDB = true
			goto Retry
		}
		return nil, err
	}

	return cleanup, tx.Commit()
}

// RollbackUnlessCommitted rolls back the transaction unless it has
// already been committed.
func RollbackUnlessCommitted(ctx context.Context, log Logger, tx Tx) error {
	if err := tx.Rollback(); err != nil {
		if errors.Is(err, sql.ErrTxDone) {
			log.Debug(ctx, "transaction already committed")
			return nil
		}
		log.Warn(ctx, "transaction rollback failed", "error", err)
		return err
	}
	log.Debug(ctx, "transaction rolled back")
	return nil
}

// SetSearchPathTo changes the search_path to the given schema within
// the transaction.
func SetSearchPathTo(ctx context.Context, tx MinimalTx, schema string) error {
	return tx.ExecContext(ctx, fmt.Sprintf("SET search_path TO %s", quoteIdentifier(schema)))
}

func migrateToLatest(ctx context.Context, log Logger, tx MinimalTx, migrations []Migration, freshDB bool) error {
	var (
		actualVersion int
		err           error
	)

	if !freshDB {
		actualVersion, err = tx.LatestSchemaVersion(ctx)
		if err != nil {
			return err
		}
	}

	newVersion := migrations[len(migrations)-1].Version

	if newVersion < actualVersion {
		return fmt.Errorf("actual version (%d) is higher than the number of migrations (%d); "+
			"this usually means that this build is older than expected", actualVersion, newVersion)
	}

	for _, m := range migrations {
		if m.Version <= actualVersion {
			continue
		}

		for i, query := range m.Queries {
			if err = tx.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("migration %d failed at query %d: %w", m.Version, i, err)
			}
		}

		if err = tx.CreateSchema(ctx, m.Version, m.VersionComment); err != nil {
			return fmt.Errorf("migration %d failed to update schema version: %w", m.Version, err)
		}
	}

	log.Info(ctx, "migrations completed", "previous", actualVersion, "new", newVersion)

	return nil
}

func createSchema(ctx context.Context, db DB, tx MinimalTx, schema string, temporary bool) (cleanup func() error, _ error) {
	escaped := quoteIdentifier(schema)

	if err := tx.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", escaped)); err != nil {
		return nil, err
	}

	if err := SetSearchPathTo(ctx, tx, schema); err != nil {
		return nil, err
	}

	if temporary {
		cleanup = func() error {
			return db.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", escaped))
		}
	} else {
		cleanup = func() error { return nil }
	}

	return cleanup, nil
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
