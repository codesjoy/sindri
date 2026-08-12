-- MySQL 8.0+ schema for sequence allocation and route snapshots.
-- +goose Up
-- +goose StatementBegin
CREATE TABLE sequence_ranges (
  sequence_key varchar(256) NOT NULL,
  max_id bigint NOT NULL,
  updated_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (sequence_key),
  CONSTRAINT ck_sequence_ranges_max_id CHECK (max_id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE sequence_routes (
  version bigint NOT NULL,
  payload json NOT NULL,
  created_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (version),
  CONSTRAINT ck_sequence_routes_version CHECK (version > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS sequence_routes;
DROP TABLE IF EXISTS sequence_ranges;
-- +goose StatementEnd
