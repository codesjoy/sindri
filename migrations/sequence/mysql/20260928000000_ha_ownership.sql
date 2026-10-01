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

-- MySQL 8.0+ ownership authority for the shared strong-consistency
-- high-availability protocol (docs/sequence-ha-architecture.md section 6).
--
-- Mirrors the PostgreSQL migration. The lease clock is CURRENT_TIMESTAMP(6):
-- unlike PostgreSQL, MySQL evaluates now() at statement start and re-evaluates
-- it per statement, so a read taken after the row lock is granted reflects the
-- post-wait time. SYSDATE(6) is deliberately not used because it can move
-- mid-statement.
--
-- Note: the key column keeps the name sequence_key because `key` is reserved.
-- +goose Up
-- +goose StatementBegin
ALTER TABLE sequence_ranges ADD COLUMN namespace varchar(64) NOT NULL DEFAULT '';
-- The max_id CHECK has to go before the rename: MySQL refuses to rename a
-- column a check constraint references.
ALTER TABLE sequence_ranges DROP CHECK ck_sequence_ranges_max_id;
ALTER TABLE sequence_ranges CHANGE max_id reserved_end bigint NOT NULL;
ALTER TABLE sequence_ranges ADD CONSTRAINT ck_sequence_ranges_reserved_end CHECK (reserved_end > 0);
ALTER TABLE sequence_ranges DROP PRIMARY KEY,
  ADD PRIMARY KEY (namespace, sequence_key);

CREATE TABLE slot_ownership (
  slot_id int NOT NULL,
  owner_node_id varchar(256) NULL,
  owner_instance_id varchar(256) NULL,
  epoch bigint NOT NULL DEFAULT 0,
  granted_at timestamp(6) NULL DEFAULT NULL,
  state varchar(16) NOT NULL DEFAULT 'UNOWNED',
  updated_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (slot_id),
  CONSTRAINT ck_slot_ownership_state CHECK (state IN ('OWNED', 'UNOWNED')),
  CONSTRAINT ck_slot_ownership_epoch CHECK (epoch >= 0),
  CONSTRAINT ck_slot_ownership_owner CHECK ((state = 'OWNED') = (owner_instance_id IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Seeds slot 0..16383 without a recursive CTE, whose default depth (1000) is
-- below the slot count and whose session variable is not reliably settable from
-- a migration block.
INSERT INTO slot_ownership (slot_id)
SELECT d0.n + d1.n * 16 + d2.n * 256 + d3.n * 4096
FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
      UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
      UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
      UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15) AS d0
CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
      UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
      UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
      UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15) AS d1
CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
      UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
      UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
      UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15) AS d2
CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS d3;

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

CREATE TABLE sequence_route_state (
  id smallint NOT NULL,
  revision bigint NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT ck_sequence_route_state_singleton CHECK (id = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Seeded above every route already published so a materialised snapshot can
-- never carry a version lower than one a client has already accepted.
INSERT INTO sequence_route_state (id, revision)
SELECT 1, COALESCE(MAX(version), 0) + 1 FROM sequence_routes;
-- +goose StatementEnd

-- +goose Down
-- WARNING: destructive. Reverting collapses the namespace dimension to the
-- pre-migration (sequence_key)-only primary key, so it refuses to run while any
-- non-default namespace exists rather than silently dropping rows. MySQL has no
-- SIGNAL outside a stored program, so the guard is a CHECK violation on a
-- temporary table.
-- +goose StatementBegin
CREATE TEMPORARY TABLE tmp_sequence_ns_guard (
  ok tinyint NOT NULL,
  CONSTRAINT ck_tmp_sequence_ns_guard CHECK (ok = 0)
) ENGINE=InnoDB;

INSERT INTO tmp_sequence_ns_guard (ok)
SELECT IF((SELECT COUNT(*) FROM sequence_ranges WHERE namespace <> '') = 0, 0, 1);

DROP TEMPORARY TABLE tmp_sequence_ns_guard;

DROP TABLE IF EXISTS sequence_route_state;
DROP TABLE IF EXISTS ownership_outbox;
DROP TABLE IF EXISTS slot_ownership;

ALTER TABLE sequence_ranges DROP CHECK ck_sequence_ranges_reserved_end;
ALTER TABLE sequence_ranges DROP PRIMARY KEY,
  CHANGE reserved_end max_id bigint NOT NULL,
  DROP COLUMN namespace,
  ADD PRIMARY KEY (sequence_key);
ALTER TABLE sequence_ranges ADD CONSTRAINT ck_sequence_ranges_max_id CHECK (max_id > 0);
-- +goose StatementEnd
