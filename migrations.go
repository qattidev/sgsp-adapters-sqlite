package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"io/fs"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrations embed.FS

var ErrInvalidDatabase = errors.New("sgsp sqlite: nil database")

// ApplyMigrations explicitly applies the embedded Goose migrations. Run this
// once during deployment, before starting application instances. The caller
// owns the database and its connection configuration.
func ApplyMigrations(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrInvalidDatabase
	}
	source, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, source)
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}
