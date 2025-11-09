package migrationssqlite3

import (
	"embed"
)

var (
	//go:embed *.sql
	FS embed.FS
)
