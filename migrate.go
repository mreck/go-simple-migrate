package gosimplemigrate

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type Migration struct {
	Key    string
	Script string
}

var (
	ErrDBNotSupported = errors.New("database not supported")
)

func CreateMigrationsFromEmbedFS(fs embed.FS) ([]Migration, error) {
	dents, err := fs.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("reading FS failed: %w", err)
	}

	var m []Migration

	for _, dent := range dents {
		if dent.IsDir() {
			continue
		}

		b, err := fs.ReadFile(dent.Name())
		if err != nil {
			return nil, fmt.Errorf("reading file '%s' failed: %w", dent.Name(), err)

		}

		m = append(m, Migration{
			Key:    dent.Name(),
			Script: string(b),
		})
	}

	slices.SortFunc(m, func(a, b Migration) int {
		return strings.Compare(a.Key, b.Key)
	})

	return m, nil
}

func CreateMigrationsFromDir(dir string) ([]Migration, error) {
	dents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading dir failed: %w", err)
	}

	var m []Migration

	for _, dent := range dents {
		if dent.IsDir() {
			continue
		}
		if strings.ToLower(filepath.Ext(dent.Name())) != ".sql" {
			continue
		}

		b, err := os.ReadFile(dent.Name())
		if err != nil {
			return nil, fmt.Errorf("reading file '%s' failed: %w", dent.Name(), err)
		}

		m = append(m, Migration{
			Key:    dent.Name(),
			Script: string(b),
		})
	}

	slices.SortFunc(m, func(a, b Migration) int {
		return strings.Compare(a.Key, b.Key)
	})

	return m, nil
}

func Migrate(ctx context.Context, driverName string, db *sql.DB, ms []Migration) error {
	var migrationsFilename string
	switch driverName {
	case "sqlite3":
		migrationsFilename = "sqlite.migrations.sql"
	default:
		return ErrDBNotSupported
	}

	err := db.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("pinging db failed: %w", err)
	}

	b, err := migrationsFS.ReadFile(migrationsFilename)
	if err != nil {
		return fmt.Errorf("reading migrations table script failed: %w", err)
	}

	cCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := db.BeginTx(cCtx, nil)
	if err != nil {
		return fmt.Errorf("creating transaction failed: %w", err)
	}

	_, err = tx.Exec(string(b))
	if err != nil {
		return fmt.Errorf("running migration table script failed: %w", err)
	}

	for _, m := range ms {
		var n int
		err := tx.QueryRow(`SELECT COUNT(*) FROM "migrations" WHERE "filename" = ?`, m.Key).Scan(&n)
		if err != nil {
			return fmt.Errorf("selecting from migrations table failed: %w", err)
		}
		if n > 0 {
			continue
		}

		_, err = tx.Exec(m.Script)
		if err != nil {
			return fmt.Errorf("running migration '%s' failed: %w", m.Key, err)
		}

		_, err = tx.Exec(`INSERT INTO "migrations" ("filename") VALUES (?)`, m.Key)
		if err != nil {
			return fmt.Errorf("updating migrations table with '%s' failed: %w", m.Key, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("comitting migration transaction failed: %w", err)
	}

	return nil
}
