# Sequence quick start

This guide starts the self-contained PostgreSQL stack, publishes a single-node
route, and calls the Sequence gRPC API. The stack includes PostgreSQL, database
migrations, Sequence, an OpenTelemetry Collector, Prometheus, Tempo, and
Grafana.

Sequence returns IDs that are strictly increasing for each key. IDs are not
guaranteed to be contiguous: unused values from a reserved range can become gaps
after a restart, route handoff, or idle-state eviction. A single request can ask
for a contiguous block with `count`, and `FetchNextBatch` allocates for many keys
at once.

## Prerequisites

- Docker with the Compose plugin
- `grpcurl` for the RPC examples
- A checkout of this repository

Run every command from the repository root.

## 1. Start the stack

Create the local environment file and start the default stack:

```sh
cp deploy/docker/.env.example deploy/docker/.env
docker compose -f deploy/docker/compose.yaml up --build -d
docker compose -f deploy/docker/compose.yaml ps
```

The migration container creates `sequence_ranges`, `sequence_routes`,
`slot_ownership`, `ownership_outbox`, `sequence_route_state`,
`sequence_node_liveness` and `sequence_coordinator` before Sequence starts, and
pre-creates one `slot_ownership` row per routing slot. The local stack runs
Sequence as `mode: both`: the data plane claims and serves slots, and the
publisher half materialises the first directory as soon as there is ownership to
snapshot. Until a directory exists `GetRoute` returns
`SEQUENCE_ROUTE_UNAVAILABLE` and allocation remains paused.

Placement is computed locally, by every node, from the rows the fleet shares.
The right to hand out IDs still comes from `slot_ownership`, so a node claims
the slots its own plan hands it: an unowned slot is granted immediately, while a
slot still held by another instance is only taken over once the quiet window
`ha.quiet_window` has elapsed since that instance's last grant, unless its owner
released it first. A node that loses a slot stops serving it, waits for its
in-flight allocations to finish and then releases it with an `epoch`-CAS, which
lets the next owner start without waiting for the window.

Inspect startup if a container does not become ready:

```sh
docker compose -f deploy/docker/compose.yaml logs migrate sequence
```

## 2. Wait for the directory to place itself

There is no central placement decision to wait for. Each node reads
`slot_ownership` and `sequence_node_liveness` on every heartbeat and computes
which of the 16,384 slots its own node id should hold. A slot nobody owns is
assigned deterministically across the live node ids; a slot whose owner is gone
is reassigned to a node that is still there; and a slot whose owner came back
under the same node id but a new process stays with that node id, so a restart
reclaims the position it left. The node then claims what it planned, and the
publisher half of the process materialises the result into
`sequence_routes` for clients to follow.

Watch a node converge:

```sh
docker compose -f deploy/docker/compose.yaml logs --since 30s sequence
```

Three rules are worth knowing when reading that log:

- A node only ever takes a slot nobody owns, or one whose owner cannot serve it:
  a dead node id, a node whose grant aged past the quiet window, or a restarted
  process under the same node id. Adding a node does not move a serving slot.
- To move slots off a node, stop it. It drains and releases what it holds on the
  way down, and the nodes that remain pick those slots up on their next
  heartbeat. That is what makes a rollout unable to lose an id, and it is why no
  orchestrator event is ever consulted for ownership.
- The publisher decides nothing. It snapshots the authority under a coordinator
  lease and advances the published revision, so losing it delays what clients
  see but never stops a node from owning or serving a slot.

Deployments differ in how the two halves are run: the local stack uses
`mode: both`, while a production StatefulSet usually runs its members as
`mode: data` and one or more separate replicas as `mode: control`. The mode is
selected by `app.sequence.mode`, `SKULD_SEQUENCE_MODE`, or `--mode`, and a
missing or invalid value fails the start.

See section 10.7 of [sequence-ha-architecture.md](sequence-ha-architecture.md) for
the parameters, the metrics the process exports, and the fencing that keeps the
publisher's writes honest.

## 3. Build an RPC descriptor

The gRPC server does not expose reflection. Build a descriptor set from the
checked-in Protocol Buffer sources with the repository-pinned Buf binary:

```sh
./bin/buf build api \
  --as-file-descriptor-set \
  --output /tmp/sindri-sequence.protoset
```

If the pinned tools are missing, install them first with `make tools.install`.

## 4. Verify the route

Call `GetRoute` until it returns a revision and node `sequence-1` rather than
`SEQUENCE_ROUTE_UNAVAILABLE`:

```sh
grpcurl -plaintext \
  -protoset /tmp/sindri-sequence.protoset \
  -d '{"knownVersion":"0"}' \
  localhost:19010 \
  codesjoy.sindri.sequence.v1.SequenceGenerator/GetRoute
```

The response includes all 16,384 slots, so it is long. The node lists are the
projection a router uses; `segments` is the authority they were derived from,
one entry per run of slots sharing an owner instance and epoch. Its beginning
should look like this:

```json
{
  "route": {
    "version": "1",
    "layoutVersion": "1",
    "nodes": [
      {
        "nodeId": "sequence-1",
        "slots": [0, 1, 2]
      }
    ],
    "segments": [
      {
        "startSlot": 0,
        "endSlot": 16383,
        "ownerNodeId": "sequence-1",
        "ownerInstanceId": "6f1c0a2b8d4e5f60",
        "slotEpoch": "7"
      }
    ]
  }
}
```

A snapshot that carries no segments is refused rather than followed: without an
epoch per slot a node cannot check a stale owner, so the only safe answer is to
keep the last valid directory and report it as unavailable.

## 5. Read the high-availability report

The report is registered on the governor's admin listener, which binds loopback.
It reports state rather than gating traffic — nothing routes by it, and no
orchestrator needs to reach it — so read it where it lives:

```sh
curl -sS http://127.0.0.1:8080/healthz
```

Under the compose stack that admin port is not published on purpose; reach it
from inside the container as `deploy/docker/README.md` shows.

Before any route is published the response is `503` with `"reason":"initializing"`.
Once the route from step 4 is loaded it becomes `200`:

```json
{
  "ready": true,
  "reason": "serving",
  "data": {
    "ready": true,
    "reason": "serving",
    "quiet_window_seconds": 5,
    "lease_duration_seconds": 3,
    "max_pause_seconds": 1
  },
  "control": {
    "ready": true,
    "reason": "serving",
    "last_pass_failed": false,
    "passes": 42,
    "unowned_slots": 0,
    "revision": 7
  }
}
```

The body reports each half the process was told to run, and the top-level `ready`
is their conjunction: a `data` process reports only `data`, a `control` process
only `control`, and a `both` process reports both. That is what makes a combined
process visible as its own failure: a process that has stopped publishing keeps
answering from the last directory, and the probe has to say so.

The `data` half carries the bounds in force (quiet window, lease duration and the
asserted pause bound), so a probe failure and a dashboard can be read against
each other. Losing the database does **not** flip that half to unready: the
affected requests already fail closed with their own retriable reason, and a
restart would not repair the database.

The `control` half reports `stalled` when no pass has completed for three pass
intervals, and stays ready while passes are failing, with `last_pass_failed`
telling the two apart. `passes`, `unowned_slots` and `revision` are carried next
to the verdict so convergence can be read from it.

## 6. Allocate IDs

Request the next ID for a key:

```sh
grpcurl -plaintext \
  -protoset /tmp/sindri-sequence.protoset \
  -d '{"key":"orders"}' \
  localhost:19010 \
  codesjoy.sindri.sequence.v1.SequenceGenerator/FetchNext
```

Run the command again with the same key. The second `id` must be greater than
the first. Different keys have independent sequences. Keys must contain between
1 and 256 bytes. The response `id` is the first ID of the block and `count` is
its length.

Request five contiguous IDs at once by setting the optional `count`:

```sh
grpcurl -plaintext \
  -protoset /tmp/sindri-sequence.protoset \
  -d '{"key":"orders","count":5}' \
  localhost:19010 \
  codesjoy.sindri.sequence.v1.SequenceGenerator/FetchNext
```

`count` defaults to `1` and accepts `1..10000`. Consecutive calls return
blocks that do not overlap, but the next block is not guaranteed to start right
after the previous one.

Allocate for several keys at once with `FetchNextBatch`. Every key must be owned
by the same node, so send the keys that share an owner; the response preserves
request order:

```sh
grpcurl -plaintext \
  -protoset /tmp/sindri-sequence.protoset \
  -d '{"requests":[{"key":"orders"},{"key":"invoices","count":3}]}' \
  localhost:19010 \
  codesjoy.sindri.sequence.v1.SequenceGenerator/FetchNextBatch
```

A batch accepts at most 1000 keys, each key may request at most 10000 IDs, and
one request may cover at most 100000 IDs. Duplicate keys, keys routed to
different owners, and keys this node does not own are rejected. A failed batch
returns no partial results, but IDs already consumed by that attempt can become
gaps.

Applications that use multiple Sequence nodes should use the route-aware Go
integration in `github.com/codesjoy/sindri/pkg/sequence`. It refreshes route
snapshots, selects the node that owns the key's slot, and retries one stale-route
failure. `sequence.NewBatchClient(router, client).FetchNext(ctx, requests)` takes
`[]sequence.KeyRequest` and groups the keys by owner, so one owner needs one
`FetchNextBatch` call; a group that does not succeed fails the whole call. The
new client checks the response `count` and does not silently downgrade: an old
server that returns `count=0` is only treated as a single ID, and batches against
a server without `FetchNextBatch` fail with an explicit compatibility error. The
generated request and response types live in
`github.com/codesjoy/sindri/gen/go/sequence/v1`.

## Configuration

The checked-in service configuration expands the existing `SKULD_*` environment
variables. The default Compose stack recognizes these primary settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `SKULD_SEQUENCE_APP_NAME` | `github.com.codesjoy.skuld.sequence` | Yggdrasil application identity used for service discovery and telemetry |
| `SKULD_SEQUENCE_MODE` | `both` | Startup shape: `data`, `control`, or `both`. Missing or invalid values fail the start; `--mode` overrides it |
| `SKULD_SEQUENCE_DRIVER` | `postgres` | Database dialect: `postgres` or `mysql` |
| `SKULD_SEQUENCE_DSN` | Local PostgreSQL DSN | Complete service database connection string |
| `SKULD_SEQUENCE_NODE_ID` | `sequence-1` | Node ID referenced by route snapshots |
| `SKULD_SEQUENCE_GRPC_PORT` | `19010` | Host port for the Sequence gRPC server |
| `SKULD_SEQUENCE_DB_PASSWORD` | `skuld-local` | Local PostgreSQL owner password |
| `SKULD_SEQUENCE_MEMORY_LIMIT` | `1g` | Container memory limit |
| `SKULD_OTLP_ENDPOINT` | `otel-collector:4317` | OTLP gRPC endpoint visible to Sequence |
| `GRAFANA_PORT` | `3000` | Host port for Grafana |

The `app.sequence.dataplane.allocator` section also controls range sizing and
adaptive prefetch. `prefetch_ratio` (default `0.5`) is the fallback watermark
while the local rate estimate is not ready. `prefetch_latency_multiplier`
(default `4`, range `(1,10]`) multiplies the trusted local reserve p99 to decide
how early to prefetch; fewer than `prefetch_latency_min_samples` successful
samples (default `100`) within `prefetch_latency_window` (default `5m`) falls
back to `reserve_timeout`. `prefetch_rate_reset_after` (default `1m`) restarts a
key's rate estimate after an idle gap.

Placement bounds live under `app.sequence.dataplane.ha`: `node_ttl` (default
`15s`) is how long a node's liveness row counts as current, and it must exceed
`ticker.heartbeat_ticks * ticker.base_tick_interval` by a wide margin.
`app.sequence.controlplane` configures the publisher: `layout_version` identifies
the slot layout a snapshot is minted under, `coordinator_lease` is how long one
replica keeps the publisher role, and `reconcile_interval` and `pass_timeout`
bound the cadence and length of one publish pass.

`reserve_timeout` (default `1s`) bounds **one** storage statement — a range
reservation, one lease renewal, or one claim or release batch — not a whole
operation. An operation that spans several statements (a reservation for many
keys, a claim that crosses the 1000-slot batch size) gives each statement its own
bound, so a slow statement cannot leave the ones after it without any budget.

Each key has at most one background prefetch or retry in flight. A failed
background reservation leaves the active range usable and schedules a jittered
exponential retry; if the range is exhausted first, the request performs one
synchronous final reservation and returns the database error when that also
fails. Route removal and idle cleanup cancel pending retries. Prefetch outcomes
and the current reserve p99 are exported without key labels.

Set a distinct application name for each independent deployment, for example
`github.com.codesjoy.skuld.sequence.user` and
`github.com.codesjoy.skuld.sequence.group`. The name is resolved once at process
startup; it is not part of `app.sequence` and cannot be changed by a config
reload. Consumers must use the same name in their Yggdrasil
`clients.services` entry and when calling `NewClient`.

Application names separate service registration, discovery, and application
identity telemetry. They do not namespace database records. Each independent
deployment must use its own DSN; deployments that share a database also share
the `sequence_ranges` and `sequence_routes` tables.

Do not commit real credentials or production DSNs. See the
[Docker deployment guide](../deploy/docker/README.md) for MySQL DSNs, external
dependencies, runtime memory sizing, pprof access, and image overrides.

## Stop or reset the stack

Stop the containers while retaining database and observability data:

```sh
docker compose -f deploy/docker/compose.yaml down
```

Delete the local volumes only when a full reset is intended:

```sh
docker compose -f deploy/docker/compose.yaml down -v
```

The second command permanently removes the local PostgreSQL, Prometheus, Tempo,
and Grafana data managed by this Compose project.
