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
  event_id bigint NOT NULL AUTO_INCREMENT,
  slot_id int NOT NULL,
  owner_node_id varchar(256) NULL,
  owner_instance_id varchar(256) NULL,
  epoch bigint NOT NULL,
  event_type varchar(16) NOT NULL,
  created_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
-- +goose StatementEnd
