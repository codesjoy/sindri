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

-- This baseline is for empty databases, not an upgrade of the previous schema.
-- +goose Up
-- +goose StatementBegin
CREATE TABLE sequence_ranges (
  sequence_key varbinary(256) PRIMARY KEY,
  reserved_end bigint NOT NULL CHECK (reserved_end > 0),
  updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE sequence_instance_leases (
  instance_id varchar(256) PRIMARY KEY,
  node_id varchar(256) NOT NULL CHECK (node_id <> ''),
  ownership_revision bigint NOT NULL DEFAULT 0 CHECK (ownership_revision >= 0),
  granted_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  state varchar(16) NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'RETIRED')),
  created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE sequence_slot_ownership (
  slot_id integer PRIMARY KEY CHECK (slot_id >= 0 AND slot_id < 16384),
  owner_instance_id varchar(256),
  epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  state varchar(16) NOT NULL DEFAULT 'UNOWNED' CHECK (state IN ('UNOWNED', 'OWNED', 'DRAINING')),
  updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  FOREIGN KEY (owner_instance_id) REFERENCES sequence_instance_leases(instance_id),
  CHECK ((state = 'UNOWNED' AND owner_instance_id IS NULL) OR
         (state IN ('OWNED', 'DRAINING') AND owner_instance_id IS NOT NULL AND epoch > 0)),
  INDEX ix_sequence_slot_ownership_instance (owner_instance_id, slot_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
INSERT INTO sequence_slot_ownership (slot_id)
SELECT a.n + 16*b.n + 256*c.n + 4096*d.n
FROM (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15) a
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15) b
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15) c
CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) d;

CREATE TABLE sequence_node_liveness (
  node_id varchar(256) PRIMARY KEY,
  instance_id varchar(256) NOT NULL,
  last_seen_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  eligible_since datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  INDEX ix_sequence_node_liveness_seen (last_seen_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE sequence_coordinator (
  id smallint PRIMARY KEY CHECK (id = 1),
  owner_instance_id varchar(256),
  epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  expires_at datetime(6),
  updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
INSERT INTO sequence_coordinator (id) VALUES (1);

CREATE TABLE sequence_route_snapshot (
  id smallint PRIMARY KEY CHECK (id = 1),
  version bigint NOT NULL CHECK (version > 0),
  payload json NOT NULL,
  updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE sequence_slot_handoffs (
  slot_id integer PRIMARY KEY,
  handoff_id varchar(256) NOT NULL,
  kind varchar(16) NOT NULL CHECK (kind IN ('TRANSFER', 'RELEASE')),
  source_instance_id varchar(256) NOT NULL,
  source_epoch bigint NOT NULL CHECK (source_epoch > 0),
  target_instance_id varchar(256),
  phase varchar(16) NOT NULL CHECK (phase IN ('PLANNED', 'READY', 'DRAINING', 'TRANSFERRED', 'COMPLETED', 'CANCELLED')),
  drained boolean NOT NULL DEFAULT false,
  target_ready boolean NOT NULL DEFAULT false,
  not_before datetime(6),
  created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  FOREIGN KEY (slot_id) REFERENCES sequence_slot_ownership(slot_id),
  CHECK ((kind = 'TRANSFER' AND target_instance_id IS NOT NULL) OR
         (kind = 'RELEASE' AND target_instance_id IS NULL)),
  CHECK (phase NOT IN ('DRAINING', 'TRANSFERRED') OR not_before IS NOT NULL),
  INDEX ix_sequence_slot_handoffs_source (source_instance_id, phase, slot_id),
  INDEX ix_sequence_slot_handoffs_target (target_instance_id, phase, slot_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
-- +goose StatementEnd

-- Stop every sequence process before dropping the authority schema.
-- +goose Down
-- +goose StatementBegin
DROP TABLE sequence_slot_handoffs;
DROP TABLE sequence_route_snapshot;
DROP TABLE sequence_coordinator;
DROP TABLE sequence_node_liveness;
DROP TABLE sequence_slot_ownership;
DROP TABLE sequence_instance_leases;
DROP TABLE sequence_ranges;
-- +goose StatementEnd
