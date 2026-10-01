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

-- Placement control for the shared strong-consistency protocol
-- (docs/sequence-ha-architecture.md section 10.4 and appendix D.4).
--
-- slot_ownership stays the only authority for who may hand out ids. This table
-- elects the one replica that materialises the directory clients follow; the
-- placement itself is computed locally by every node from slot_ownership plus
-- sequence_node_liveness, so there is no intent table to keep in step with
-- either of them.
--
-- Losing the coordinator row is recoverable: the next pass of any live replica
-- takes the lease and republishes the view, which is why the Down section needs
-- no guard.

-- +goose Up
-- +goose StatementBegin
CREATE TABLE sequence_coordinator (
  id smallint NOT NULL,
  owner_instance_id varchar(256) NULL,
  epoch bigint NOT NULL DEFAULT 0,
  expires_at timestamp(6) NULL DEFAULT NULL,
  updated_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  CONSTRAINT ck_sequence_coordinator_singleton CHECK (id = 1),
  CONSTRAINT ck_sequence_coordinator_epoch CHECK (epoch >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO sequence_coordinator (id) VALUES (1);
-- +goose StatementEnd

-- +goose Down
-- Reverting removes the publisher election, so new revisions stop being
-- materialised: the fleet keeps running on the last snapshot, and a node that
-- claims a slot locally becomes visible to clients only when the publisher is
-- restored.
-- +goose StatementBegin
DROP TABLE IF EXISTS sequence_coordinator;
-- +goose StatementEnd
