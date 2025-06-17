package gosimplemigrate_test

import (
	"context"
	"database/sql"
	"embed"
	"os"
	"sort"
	"testing"

	smig "github.com/mreck/go-simple-migrate"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

var (
	//go:embed *.sql
	sqlFS embed.FS
)

func TestCreateMigrationsFromEmbedFS(t *testing.T) {
	expected := []smig.Migration{
		{"sqlite.migrations.sql", ""},
	}

	for i, e := range expected {
		b, err := os.ReadFile(e.Key)
		assert.Nil(t, err)
		e.Script = string(b)
		expected[i] = e
	}

	m, err := smig.CreateMigrationsFromEmbedFS(sqlFS)
	assert.Nil(t, err)
	assert.Equal(t, expected, m)
}

func TestMigrateFS(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)

	m := []smig.Migration{
		{"1", "CREATE TABLE test_1 (id INT)"},
		{"2", "CREATE TABLE test_2 (id INT)"},
	}

	for range 2 {
		err := smig.Migrate(ctx, db, m)
		assert.Nil(t, err)
		tables, err := getTables(db)
		assert.Nil(t, err)
		assert.Equal(t, []string{"migrations", "test_1", "test_2"}, tables)
	}

	m = append(m, smig.Migration{"3", "CREATE TABLE test_3 (id INT)"})

	for range 2 {
		err := smig.Migrate(ctx, db, m)
		assert.Nil(t, err)
		tables, err := getTables(db)
		assert.Nil(t, err)
		assert.Equal(t, []string{"migrations", "test_1", "test_2", "test_3"}, tables)
	}
}

func getTables(db *sql.DB) ([]string, error) {
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
