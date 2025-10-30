`simplemigrations` helps you run ordered database migrations in Go while
staying agnostic to the underlying transport. You provide transactional
primitives and the package coordinates schema upgrades.

## core ideas

- implement `MinimalTx`
    - it executes migration statements and records schema versions
- you can optionally implement `Tx` and `DB` for integration with PostgreSQL
    - when present, the library can create schemas and manage retries through `MigrateToLatestWithSchema`
- you can also supply a `Logger` to observe migration progress
    - `NopLogger` is available when you do not need logging

`MigrateToLatest` is the general entry point. Use
`MigrateToLatestWithSchema` only with PostgreSQL-backed implementations
that expose the additional `DB`/`Tx` methods.

## getting started

1. implement `MinimalTx` (and optionally `Tx`, `DB`, `Logger`)
2. build a slice of `Migration` values ordered by version
3. call `MigrateToLatest` with your transaction, migrations, and logger of choice

See `simplemigrations_test.go` for a runnable example showing the
expected behaviours.
