-- Copyright 2026 Codesjoy
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
