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
