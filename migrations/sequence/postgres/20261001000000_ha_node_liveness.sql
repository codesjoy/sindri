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
-- Every node reads this table to decide which node ids its slots may live on. It
-- replaces the placement session the control plane used to hold with each node:
-- a node renews one row inside the heartbeat it already sends, through the same
-- storage-backed lease primitive its slot ownership uses, and every node plans
-- from the fleet's liveness in the database it is already connected to. The gain
-- is that no process needs a channel to any other, and the whole fleet computes
-- the same placement from the same rows.
--
-- last_seen_at is written by the storage clock, never by the node's, for the
-- same reason slot_ownership.granted_at is: expiry is a comparison against
-- node_ttl, and a node clock on one side of that comparison would make liveness
-- depend on every node's time being right.
--
-- No sweep is needed. node_id is the primary key, so the row count is bounded by
-- the fleet size and a node that never comes back leaves exactly one stale row
-- rather than a growing tail. A graceful shutdown deletes its own row, which is
-- what keeps a rolling update from waiting out a full node_ttl.
-- +goose Up
-- +goose StatementBegin
CREATE TABLE sequence_node_liveness (
  node_id varchar(256) PRIMARY KEY,
  instance_id varchar(256) NOT NULL,
  last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- +goose StatementEnd

-- +goose Down
-- Reverting removes the live node set, so nodes stop being able to plan: the
-- fleet keeps serving the slots it holds, and placement resumes when the table
-- is restored.
-- +goose StatementBegin
DROP TABLE IF EXISTS sequence_node_liveness;
-- +goose StatementEnd
