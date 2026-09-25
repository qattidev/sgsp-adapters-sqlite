# SGSP sqlite assignment store

Independent Go module `qattidev/sgsp-sqlite` implementing
`qattidev/sgsp/placement.AssignmentStore`. SGSP does not import this module.
The application owns the database pool, explicitly calls `ApplyMigrations`,
constructs `New(db)`, and injects the store into `placement.BootstrapConfig`.
`Store.Close` closes a group; the application calls `db.Close()` at shutdown.

## Development

This repository is checked out as the SGSP submodule `adapters/sqlite`.
Its local `replace qattidev/sgsp => ../..` supports that unpublished layout.
For a standalone checkout, change that replacement to your SGSP checkout;
when publishing, replace the development dependency with a released SGSP version.
No database dependencies are added to the SGSP module.

```sh
go generate ./...  # pinned SQLC v1.30.0; generated code is committed
go test -race ./...
go vet ./...
make check-generated
```

Edit `queries/*.sql` and Goose migrations under `migrations/`, then regenerate.
SQLC is a development tool, not a runtime dependency. Runtime dependencies are
SGSP and Goose; the database driver below is only imported by tests and host
applications. Migrations are embedded and run through an instance-local Goose
provider (no global Goose configuration). Run `ApplyMigrations` once as a
serialized deployment step before starting application instances. `New` never
creates or migrates tables. Storage errors propagate without memory fallback.

Assignments are keyed by application ID and group key, with a version invariant.
Assignment never replaces a winner or reopens a closed group. Closure checks
both owner ID and incarnation in the transaction and commits before returning.
Tests cover independent pools and processes, competing owners, closure races, version and
owner rejection, cancellation, storage outages, reopening, and bootstrap owner
availability/restart behavior.

## SQLite configuration

Tests use `github.com/mattn/go-sqlite3`, a single driver dependency requiring
CGO and a C compiler. The store accepts `*sql.DB` so applications own driver
selection. Use a file-backed database and configure a busy timeout on every
connection. With mattn/go-sqlite3, a suitable DSN is:

```text
file:assignments.db?_busy_timeout=10000&_journal_mode=WAL&_synchronous=FULL
```

```go
import (
    "context"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    sqlite "qattidev/sgsp-sqlite"
)

// In application startup; handle each error before proceeding:
db, err := sql.Open("sqlite3", "file:assignments.db?_busy_timeout=10000&_journal_mode=WAL&_synchronous=FULL")
err = sqlite.ApplyMigrations(context.Background(), db)
store, err := sqlite.New(db)
// Inject store into placement.BootstrapConfig.Store.
```

Assign writes before reading; Close acquires the write lock with a no-op
UPDATE before checking version and owner. SQLite serializes these transactions
across connections and processes. No Go mutex or implicit reassignment is used.
The busy timeout waits for a competing writer; an exhausted timeout propagates
as a storage error. Choose a timeout appropriate for the application's request
budget. WAL requires a filesystem that supports SQLite locking. Keep database,
WAL, and shared-memory files together. Do not use a per-connection `:memory:`
database for durable storage or multi-connection tests.

Closed assignments remain persisted. Destructive Down migrations are only for
disposable databases. Test databases are real temporary files and require no
external service.

## License

Copyright 2026 Johannes Sarpola.

SGSP sqlite assignment store is licensed under the [Apache License, Version 2.0](LICENSE).
