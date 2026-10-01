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

-- The ownership outbox never had a reader. Publisher passes read the authority
-- tables on their reconciliation cadence, so retaining a row per grant and
-- release would only grow the database.
-- +goose Up
-- +goose StatementBegin
DROP TABLE IF EXISTS ownership_outbox;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE TABLE ownership_outbox (
  event_id bigserial PRIMARY KEY,
  slot_id integer NOT NULL CHECK (slot_id >= 0),
  owner_node_id varchar(256),
  owner_instance_id varchar(256),
  epoch bigint NOT NULL,
  event_type varchar(16) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- +goose StatementEnd
