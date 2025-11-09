package gosimplemigrate_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"testing"

	gsm "github.com/mreck/go-simple-migrate"
	msqlite3 "github.com/mreck/go-simple-migrate/testutils/migrations_sqlite3"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func Test_CreateMigrationsFromEmbedFS(t *testing.T) {
	m, err := gsm.CreateMigrationsFromEmbedFS(msqlite3.FS)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	assert.Equal(t, m[0].Key, "001.sql")
	assert.Equal(t, m[1].Key, "002.sql")

	assert.Equal(t, m[0].Script, mustReadFile("testutils/migrations_sqlite3/001.sql"))
	assert.Equal(t, m[1].Script, mustReadFile("testutils/migrations_sqlite3/002.sql"))
}

func Test_CreateMigrationsFromDir(t *testing.T) {
	m, err := gsm.CreateMigrationsFromDir("testutils/migrations_sqlite3")
	assert.NoError(t, err)
	assert.NotNil(t, m)

	assert.Equal(t, m[0].Key, "001.sql")
	assert.Equal(t, m[1].Key, "002.sql")

	assert.Equal(t, m[0].Script, mustReadFile("testutils/migrations_sqlite3/001.sql"))
	assert.Equal(t, m[1].Script, mustReadFile("testutils/migrations_sqlite3/002.sql"))
}

func Test_MigrateFS(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	assert.Nil(t, err)

	m, err := gsm.CreateMigrationsFromEmbedFS(msqlite3.FS)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	for i := range 3 {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			err := gsm.Migrate(ctx, "sqlite3", db, m)
			assert.Nil(t, err)

			tables, err := getTablesSqlite(db)
			assert.Nil(t, err)
			assert.Equal(t, []string{"migrations", "test_1", "test_2"}, tables)
		})
	}

	m = append(m, gsm.Migration{"3", "CREATE TABLE test_3 (id INT)"})

	for i := range 3 {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			err := gsm.Migrate(ctx, "sqlite3", db, m)
			assert.Nil(t, err)

			tables, err := getTablesSqlite(db)
			assert.Nil(t, err)
			assert.Equal(t, []string{"migrations", "test_1", "test_2", "test_3"}, tables)
		})
	}
}

func getTablesSqlite(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
		SELECT name
		FROM sqlite_schema
		WHERE type = 'table'
		AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var s string
		err := rows.Scan(&s)
		if err != nil {
			return nil, err
		}
		tables = append(tables, s)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	sort.Strings(tables)

	return tables, nil
}

func mustReadFile(filename string) string {
	b, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	return string(b)
}
