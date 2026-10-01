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

-- Live node set for slot placement (docs/sequence-ha-architecture.md section
-- 10.4).
--
-- Mirrors the PostgreSQL migration. The lease clock is CURRENT_TIMESTAMP(6), for
-- the reason the ownership migration gives: MySQL evaluates it at statement
-- start and re-evaluates it per statement, so a reading taken after the row lock
-- reflects the post-wait time, whereas SYSDATE(6) can move mid-statement.
--
-- No sweep is needed: node_id is the primary key, so rows are bounded by the
-- fleet size and a node that never returns leaves one stale row rather than a
-- growing tail.
-- +goose Up
-- +goose StatementBegin
CREATE TABLE sequence_node_liveness (
  node_id varchar(256) NOT NULL,
  instance_id varchar(256) NOT NULL,
  last_seen_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (node_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
-- +goose StatementEnd

-- +goose Down
-- Reverting removes the live node set, so nodes stop being able to plan: the
-- fleet keeps serving the slots it holds, and placement resumes when the table
-- is restored.
-- +goose StatementBegin
DROP TABLE IF EXISTS sequence_node_liveness;
-- +goose StatementEnd
