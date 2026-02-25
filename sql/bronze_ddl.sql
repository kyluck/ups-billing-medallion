-- Keep Bronze separate from everything else
CREATE SCHEMA IF NOT EXISTS bronze;

-- UUID generator
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1) One row per unique physical file (by hash)
CREATE TABLE IF NOT EXISTS bronze.file_registry (
  file_id                 UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  original_filename       TEXT NOT NULL,
  file_hash_sha256        TEXT NOT NULL UNIQUE,
  file_size_bytes         BIGINT NOT NULL CHECK (file_size_bytes >= 0),
  detected_billing_period TEXT NULL,
  first_seen_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  notes                   TEXT NULL
);

-- 2) One row per load attempt (you can retry the same file multiple times)
CREATE TABLE IF NOT EXISTS bronze.load_event (
  load_event_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  file_id         UUID NOT NULL REFERENCES bronze.file_registry(file_id),
  started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at     TIMESTAMPTZ NULL,
  status          TEXT NOT NULL DEFAULT 'STARTED'
                  CHECK (status IN ('STARTED','SUCCESS','FAILED')),
  rows_read       BIGINT NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
  rows_inserted   BIGINT NOT NULL DEFAULT 0 CHECK (rows_inserted >= 0),
  error_message   TEXT NULL,
  loader_version  TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_load_event_file_id ON bronze.load_event(file_id);
CREATE INDEX IF NOT EXISTS idx_load_event_status ON bronze.load_event(status);

-- 3) Raw invoice rows (append-only)
CREATE TABLE IF NOT EXISTS bronze.invoice_row (
  bronze_row_id BIGSERIAL PRIMARY KEY,
  file_id       UUID NOT NULL REFERENCES bronze.file_registry(file_id),
  load_event_id UUID NOT NULL REFERENCES bronze.load_event(load_event_id),
  row_number    INTEGER NOT NULL CHECK (row_number > 0),
  raw_values    JSONB NOT NULL,  -- JSON array of values (strings/null)
  ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Prevent accidental duplicate inserts from the loader
  CONSTRAINT uq_invoice_row_load_row UNIQUE (load_event_id, row_number)
);

CREATE INDEX IF NOT EXISTS idx_invoice_row_file_id ON bronze.invoice_row(file_id);
CREATE INDEX IF NOT EXISTS idx_invoice_row_load_event_id ON bronze.invoice_row(load_event_id);

