package gosimplemigrate

import (
	"embed"
)

var (
	//go:embed *migrations.sql
	migrationsFS embed.FS
)
