CREATE TABLE IF NOT EXISTS "migrations" (
    "row_id" INTEGER PRIMARY KEY NOT NULL,
    "filename" TEXT NOT NULL UNIQUE
)
