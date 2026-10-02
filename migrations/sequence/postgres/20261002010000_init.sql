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
  sequence_key varchar(256) COLLATE "C" PRIMARY KEY,
  reserved_end bigint NOT NULL CHECK (reserved_end > 0),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE sequence_instance_leases (
  instance_id varchar(256) COLLATE "C" PRIMARY KEY,
  node_id varchar(256) COLLATE "C" NOT NULL CHECK (node_id <> ''),
  ownership_revision bigint NOT NULL DEFAULT 0 CHECK (ownership_revision >= 0),
  granted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  state varchar(16) NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'RETIRED')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE sequence_slot_ownership (
  slot_id integer PRIMARY KEY CHECK (slot_id >= 0 AND slot_id < 16384),
  owner_instance_id varchar(256) COLLATE "C" REFERENCES sequence_instance_leases(instance_id),
  epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  state varchar(16) NOT NULL DEFAULT 'UNOWNED' CHECK (state IN ('UNOWNED', 'OWNED', 'DRAINING')),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK ((state = 'UNOWNED' AND owner_instance_id IS NULL) OR
         (state IN ('OWNED', 'DRAINING') AND owner_instance_id IS NOT NULL AND epoch > 0))
);
CREATE INDEX ix_sequence_slot_ownership_instance ON sequence_slot_ownership (owner_instance_id, slot_id);
INSERT INTO sequence_slot_ownership (slot_id) SELECT generate_series(0, 16383);

CREATE TABLE sequence_node_liveness (
  node_id varchar(256) COLLATE "C" PRIMARY KEY,
  instance_id varchar(256) COLLATE "C" NOT NULL,
  last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  eligible_since timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ix_sequence_node_liveness_seen ON sequence_node_liveness (last_seen_at);

CREATE TABLE sequence_coordinator (
  id smallint PRIMARY KEY CHECK (id = 1),
  owner_instance_id varchar(256) COLLATE "C",
  epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  expires_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO sequence_coordinator (id) VALUES (1);

CREATE TABLE sequence_route_snapshot (
  id smallint PRIMARY KEY CHECK (id = 1),
  version bigint NOT NULL CHECK (version > 0),
  payload jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE sequence_slot_handoffs (
  slot_id integer PRIMARY KEY REFERENCES sequence_slot_ownership(slot_id),
  handoff_id varchar(256) COLLATE "C" NOT NULL,
  kind varchar(16) NOT NULL CHECK (kind IN ('TRANSFER', 'RELEASE')),
  source_instance_id varchar(256) COLLATE "C" NOT NULL,
  source_epoch bigint NOT NULL CHECK (source_epoch > 0),
  target_instance_id varchar(256) COLLATE "C",
  phase varchar(16) NOT NULL CHECK (phase IN ('PLANNED', 'READY', 'DRAINING', 'TRANSFERRED', 'COMPLETED', 'CANCELLED')),
  drained boolean NOT NULL DEFAULT false,
  target_ready boolean NOT NULL DEFAULT false,
  not_before timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK ((kind = 'TRANSFER' AND target_instance_id IS NOT NULL) OR
         (kind = 'RELEASE' AND target_instance_id IS NULL)),
  CHECK (phase NOT IN ('DRAINING', 'TRANSFERRED') OR not_before IS NOT NULL)
);
CREATE INDEX ix_sequence_slot_handoffs_source ON sequence_slot_handoffs (source_instance_id, phase, slot_id);
CREATE INDEX ix_sequence_slot_handoffs_target ON sequence_slot_handoffs (target_instance_id, phase, slot_id);
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
