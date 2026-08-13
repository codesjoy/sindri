# Sequence quick start

This guide starts the self-contained PostgreSQL stack, publishes a single-node
route, and calls the Sequence gRPC API. The stack includes PostgreSQL, database
migrations, Sequence, an OpenTelemetry Collector, Prometheus, Tempo, and
Grafana.

Sequence returns IDs that are strictly increasing for each key. IDs are not
guaranteed to be contiguous: unused values from a reserved range can become gaps
after a restart, route handoff, or idle-state eviction.

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

The migration container creates `sequence_ranges` and `sequence_routes` before
Sequence starts. It does not publish a route. Until a valid route exists,
`GetRoute` returns `SEQUENCE_ROUTE_UNAVAILABLE` and allocation remains paused.

Inspect startup if a container does not become ready:

```sh
docker compose -f deploy/docker/compose.yaml logs migrate sequence
```

## 2. Publish the initial route

The following statement assigns all 16,384 slots to the default node,
`sequence-1`. PostgreSQL generates the slot array, so the route is complete
without a checked-in data file. Re-running the statement keeps an existing
version 1 unchanged.

```sh
docker compose -f deploy/docker/compose.yaml exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U skuld_sequence -d skuld_sequence <<'SQL'
INSERT INTO sequence_routes (version, payload)
VALUES (
  1,
  jsonb_build_object(
    'nodes',
    jsonb_build_array(
      jsonb_build_object(
        'node_id', 'sequence-1',
        'slots', (
          SELECT jsonb_agg(slot ORDER BY slot)
          FROM generate_series(0, 16383) AS slot
        )
      )
    )
  )
)
ON CONFLICT (version) DO NOTHING;
SQL
```

Sequence polls for routes once per second by default. Wait until its log shows
that route version 1 has been applied:

```sh
docker compose -f deploy/docker/compose.yaml logs --since 30s sequence
```

For a multi-node deployment, every slot must still have exactly one owner and
each route change must use a higher version. Route publication is an operator
action; it is not part of database migration.

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

Call `GetRoute` until it returns version 1 and node `sequence-1` rather than
`SEQUENCE_ROUTE_UNAVAILABLE`:

```sh
grpcurl -plaintext \
  -protoset /tmp/sindri-sequence.protoset \
  -d '{"knownVersion":"0"}' \
  localhost:19010 \
  codesjoy.sindri.sequence.v1.SequenceGenerator/GetRoute
```

The response includes all 16,384 slots, so it is long. Its beginning should look
like this:

```json
{
  "route": {
    "version": "1",
    "nodes": [
      {
        "nodeId": "sequence-1",
        "slots": [0, 1, 2]
      }
    ]
  }
}
```

## 5. Allocate IDs

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
1 and 256 bytes.

Applications that use multiple Sequence nodes should use the route-aware Go
integration in `github.com/codesjoy/sindri/pkg/sequence`. It refreshes route
snapshots, selects the node that owns the key's slot, and retries one stale-route
failure. The generated request and response types live in
`github.com/codesjoy/sindri/gen/go/sequence/v1`.

## Configuration

The checked-in service configuration expands the existing `SKULD_*` environment
variables. The default Compose stack recognizes these primary settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `SKULD_SEQUENCE_DRIVER` | `postgres` | Database dialect: `postgres` or `mysql` |
| `SKULD_SEQUENCE_DSN` | Local PostgreSQL DSN | Complete service database connection string |
| `SKULD_SEQUENCE_NODE_ID` | `sequence-1` | Node ID referenced by route snapshots |
| `SKULD_SEQUENCE_GRPC_PORT` | `19010` | Host port for the Sequence gRPC server |
| `SKULD_SEQUENCE_DB_PASSWORD` | `skuld-local` | Local PostgreSQL owner password |
| `SKULD_SEQUENCE_MEMORY_LIMIT` | `1g` | Container memory limit |
| `SKULD_OTLP_ENDPOINT` | `otel-collector:4317` | OTLP gRPC endpoint visible to Sequence |
| `GRAFANA_PORT` | `3000` | Host port for Grafana |

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
