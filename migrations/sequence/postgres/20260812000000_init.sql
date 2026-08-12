-- PostgreSQL 14+ schema for sequence allocation and route snapshots.
-- +goose Up
-- +goose StatementBegin
CREATE TABLE sequence_ranges (
  sequence_key varchar(256) PRIMARY KEY,
  max_id bigint NOT NULL CHECK (max_id > 0),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sequence_routes (
  version bigint PRIMARY KEY CHECK (version > 0),
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS sequence_routes;
DROP TABLE IF EXISTS sequence_ranges;
-- +goose StatementEnd
