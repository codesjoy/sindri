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

-- PostgreSQL 14+ ownership authority for the shared strong-consistency
-- high-availability protocol (docs/sequence-ha-architecture.md section 6).
--
-- sequence_ranges gains namespace so its primary key matches the authoritative
-- model (namespace, key); max_id is renamed to reserved_end, which is what the
-- column always held. The table is extended in place rather than replaced: the
-- existing high-watermark rows are the only record of ranges a live owner still
-- holds in memory, and a second watermark table would let one key own two
-- watermarks, which is a uniqueness (S1) violation, not a gap.
--
-- Note: the key column keeps the name sequence_key because `key` is reserved in
-- MySQL and is not worth the quoting hazard across the raw SQL in this repo.
-- +goose Up
-- +goose StatementBegin
ALTER TABLE sequence_ranges ADD COLUMN namespace varchar(64) NOT NULL DEFAULT '';
ALTER TABLE sequence_ranges DROP CONSTRAINT sequence_ranges_pkey;
ALTER TABLE sequence_ranges RENAME COLUMN max_id TO reserved_end;
ALTER TABLE sequence_ranges ADD CONSTRAINT sequence_ranges_pkey PRIMARY KEY (namespace, sequence_key);

CREATE TABLE slot_ownership (
  slot_id integer PRIMARY KEY CHECK (slot_id >= 0),
  owner_node_id varchar(256),
  owner_instance_id varchar(256),
  epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  granted_at timestamptz,
  state varchar(16) NOT NULL DEFAULT 'UNOWNED',
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT ck_slot_ownership_state CHECK (state IN ('OWNED', 'UNOWNED')),
  CONSTRAINT ck_slot_ownership_owner CHECK ((state = 'OWNED') = (owner_instance_id IS NOT NULL))
);

INSERT INTO slot_ownership (slot_id) SELECT generate_series(0, 16383);

CREATE TABLE ownership_outbox (
  event_id bigserial PRIMARY KEY,
  slot_id integer NOT NULL CHECK (slot_id >= 0),
  owner_node_id varchar(256),
  owner_instance_id varchar(256),
  epoch bigint NOT NULL,
  event_type varchar(16) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE sequence_route_state (
  id smallint PRIMARY KEY CHECK (id = 1),
  revision bigint NOT NULL
);

-- Seeded above every route already published so a materialised snapshot can
-- never carry a version lower than one a client has already accepted.
INSERT INTO sequence_route_state (id, revision)
SELECT 1, COALESCE(MAX(version), 0) + 1 FROM sequence_routes;
-- +goose StatementEnd

-- +goose Down
-- WARNING: destructive. Reverting collapses the namespace dimension to the
-- pre-migration (sequence_key)-only primary key, so it refuses to run while any
-- non-default namespace exists rather than silently dropping rows.
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM sequence_ranges WHERE namespace <> '') THEN
    RAISE EXCEPTION 'refusing to revert: non-default namespaces would be lost';
  END IF;
END $$;

DROP TABLE IF EXISTS sequence_route_state;
DROP TABLE IF EXISTS ownership_outbox;
DROP TABLE IF EXISTS slot_ownership;

ALTER TABLE sequence_ranges DROP CONSTRAINT sequence_ranges_pkey;
ALTER TABLE sequence_ranges RENAME COLUMN reserved_end TO max_id;
ALTER TABLE sequence_ranges DROP COLUMN namespace;
ALTER TABLE sequence_ranges ADD CONSTRAINT sequence_ranges_pkey PRIMARY KEY (sequence_key);
-- +goose StatementEnd
