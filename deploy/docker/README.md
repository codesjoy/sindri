# Docker Compose deployment

The default stack is self-contained: PostgreSQL, migrations, sequence, an OpenTelemetry Collector, Prometheus, Tempo, and Grafana run in one Compose project.

For a first local deployment, including initial route publication and RPC calls,
follow the [Sequence quick start](../../docs/sequence.md). This document covers
the deployment options and operational settings in more detail.

From the repository root:

```sh
cp deploy/docker/.env.example deploy/docker/.env
docker compose -f deploy/docker/compose.yaml up --build -d
```

Sequence gRPC is available on `localhost:19010`; Grafana is available on `http://localhost:3000`.
The high-availability report answers on the container's admin listener. It binds
loopback and is deliberately not published, so read it from inside the container:

```sh
docker compose -f deploy/docker/compose.yaml exec sequence \
  bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080; printf "GET /healthz HTTP/1.0\r\n\r\n" >&3; cat <&3'
```
The migration job creates the Sequence tables, including the slot ownership and
placement tables. There is no central placement decision: each node plans its
own slots from `sequence_slot_ownership` and `sequence_node_liveness` on every
heartbeat, claims what its plan hands it, and serves it. The publisher half of
the same process snapshots that authority into the single
`sequence_route_snapshot` row under a coordinator lease, so clients can follow
the moves. Allocation is paused until the first directory appears, which the
quick start shows how to watch.

One image runs every shape, selected by `SKULD_SEQUENCE_MODE` (or `--mode`):

- `both` — the local default: the data plane and the publisher in one process,
  sharing one database pool.
- `data` — allocator, heartbeat, RPC and local planning. No directory is
  published by this process.
- `control` — the publisher only: coordinator lease, route materialisation and
  control-plane readiness. It never claims a slot or renews a liveness row, and
  it does not register the Sequence RPC service.

A production StatefulSet normally runs its members as `data` and one (or a
small number of) separate Deployment replicas as `control`. The publisher only
decides how quickly a move becomes visible to clients; it is not a precondition
for a node to own or serve a slot. Losing it leaves the fleet serving the last
directory. Keep control replicas out of the Kubernetes Service or load-balancer
pool that serves Sequence RPC; clients must reach `GetRoute`, `FetchNext`, and
`FetchNextBatch` on data members. A missing or invalid mode fails the process at
startup rather than picking a shape silently. See
[../../docs/sequence.md](../../docs/sequence.md) section 2.

The two shapes differ by one setting:

```yaml
# StatefulSet member: serves ids and plans its own slots.
env:
  - name: SKULD_SEQUENCE_MODE
    value: data
  - name: SKULD_SEQUENCE_NODE_ID
    valueFrom: {fieldRef: {fieldPath: metadata.name}}
---
# Deployment replica: publishes the directory and nothing else.
env:
  - name: SKULD_SEQUENCE_MODE
    value: control
```
The sequence container has a 1 GiB hard memory limit by default. With
`app.sequence.runtime.memory_limit: auto`, sequence detects that cgroup limit
and sets the Go runtime soft limit to 80% of it. The allocator stops admitting
new keys at 90% of the Go runtime budget while continuing to serve keys already
in memory. Override the hard limit with `SKULD_SEQUENCE_MEMORY_LIMIT`; sequence
recalculates the runtime budget at startup.
The `migrate` one-shot service runs the repository-pinned goose CLI before every `sequence`
start. Goose applies only pending files from `migrations/sequence/<driver>`, so repeated runs
are safe. To inspect startup:

```sh
docker compose -f deploy/docker/compose.yaml logs -f migrate sequence
```

Stop the stack with `docker compose -f deploy/docker/compose.yaml down`. Add `-v` only when you intentionally want to delete the local PostgreSQL, Prometheus, Tempo, and Grafana data volumes.

## External dependencies

`compose.external.yaml` starts only the migration job and sequence. It does not create or manage a database, Collector, Prometheus, Tempo, or Grafana. Set complete connection values before starting it:

```sh
export SKULD_SEQUENCE_DRIVER=postgres
export SKULD_SEQUENCE_DSN='postgres://skuld_sequence:password@db.example.com:5432/skuld_sequence?sslmode=require'
export SKULD_OTLP_ENDPOINT='otel-collector.example.com:4317'
export SKULD_SEQUENCE_APP_NAME=github.com.codesjoy.skuld.sequence.user
export SKULD_SEQUENCE_MODE=both
export SKULD_SEQUENCE_NODE_ID=sequence-prod-1
export SKULD_SEQUENCE_MEMORY_LIMIT=2g
docker compose -f deploy/docker/compose.external.yaml up --build -d
```

For MySQL, use `SKULD_SEQUENCE_DRIVER=mysql` and a DSN such as `skuld_sequence:password@tcp(mysql.example.com:3306)/skuld_sequence?parseTime=true`.

The migration image contains the goose CLI and both dialect migration directories. Compose
maps the existing `SKULD_SEQUENCE_DRIVER` and `SKULD_SEQUENCE_DSN` settings to goose's
`GOOSE_DRIVER`, `GOOSE_DBSTRING`, and `GOOSE_MIGRATION_DIR` interface. Override
`SKULD_MIGRATE_IMAGE` when publishing the migration image separately from sequence.

The external Collector must accept OTLP gRPC on the configured endpoint. External Grafana is outside this Compose project; configure Prometheus and Tempo data sources there using their externally reachable URLs.

## Configuration and secrets

All `${...}` values in `configs/sequence.yaml` are expanded from the container environment. Replace the example passwords for any shared or production deployment. Prefer Docker secrets, a secret manager, or an orchestrator-provided environment over committing a `.env` file with credentials.

`SKULD_SEQUENCE_APP_NAME` is read directly by the process before Yggdrasil
loads `configs/sequence.yaml`. It defaults to
`github.com.codesjoy.skuld.sequence`. Give independent deployments distinct
names, such as `github.com.codesjoy.skuld.sequence.user` and
`github.com.codesjoy.skuld.sequence.group`, and configure consumers to use the
matching name in both their Yggdrasil `clients.services` entry and `NewClient`
call. Each deployment must also use a separate DSN: changing the application
name does not namespace `sequence_ranges` or `sequence_slot_ownership` in a
shared database.

## Memory and CPU sizing

The default runtime configuration is:

```yaml
app:
  sequence:
    runtime:
      memory_limit: auto
      auto_memory_limit_ratio: 0.8
```

In automatic mode, sequence uses the smaller of the process cgroup limit and
the machine's physical memory, then applies the configured ratio. Detection
failure is fatal; use a fixed IEC value such as `memory_limit: 1536MiB` when the
deployment does not expose either source. Fixed values and computed values must
be at least 64 MiB and less than `math.MaxInt64`.

The configured value is a Go runtime soft limit, not an operating-system hard
limit. Always pair it with a container, cgroup, or systemd memory limit. For a
hard limit `M`, the default runtime ratio of `0.8` and allocator watermark of
`0.9` stop new-key admission near `0.72M` of runtime-managed memory. A rejected
new key returns `RESOURCE_EXHAUSTED` with reason
`SEQUENCE_CAPACITY_EXHAUSTED`. Existing keys remain available, so alerts should
trigger horizontal scale-out before sustained rejection.

For backward compatibility, when `runtime.memory_limit` is absent sequence uses
a finite `GOMEMLIMIT` environment value if present; otherwise it selects
automatic mode. An explicit configuration value, including `auto`, always takes
precedence over `GOMEMLIMIT`.

Kubernetes example:

```yaml
resources:
  requests:
    cpu: "1"
    memory: 1Gi
  limits:
    cpu: "2"
    memory: 1Gi
```

For systemd, set the service cgroup limit; sequence automatically derives its
runtime budget:

```ini
[Service]
MemoryMax=2G
CPUQuota=200%
```

On a VM without a finite cgroup limit, automatic mode uses physical memory.

The existing Yggdrasil admin server exposes pprof on its loopback-bound port.
Use Kubernetes port forwarding, SSH forwarding, or an in-container client to
capture `/debug/pprof/heap`, `/debug/pprof/allocs`,
`/debug/pprof/profile`, and `/debug/pprof/goroutine`; do not publish the admin
port directly.

Do not preallocate sequence key states or add a `sync.Pool`. Key cardinality is
unknown, states are long-lived, and preallocation increases idle RSS. Tune
`allocator.cleanup_slots_per_run` only when profiles show cleanup latency or CPU
spikes; increasing it reclaims idle keys sooner at the cost of more work per
cleanup tick.

## Empty-database baseline

The candidate schema is a **single baseline migration per dialect**
(`migrations/sequence/{postgres,mysql}/20261002010000_init.sql`). It is written
for an empty database and replaces the previous chain of upgrade migrations.
There is no bridge from the older schema and no in-place upgrade: rebuild the
Sequence database into an empty one, stop every Sequence process first, and let
the migration job create the tables.

The baseline the migration job applies must match the service you are about to
run. Applying the baseline from a newer checkout under an older binary, or the
reverse, is unsupported.

## Release order

The service release is a Git marker (`service/sequence/vX.Y.Z`), and it depends
on the contract module and the SDK being published first. Publish, and verify,
in dependency order:

1. `gen/go/sequence/v0.2.0` — the generated contract module.
2. `pkg/sequence/v0.2.0` — the Go SDK, which depends on the contract.
3. `service/sequence/v0.2.0` — the service release marker.

Before the tags exist, the repository validates the candidate in isolation:

```sh
task modules:prepare SERVICE=sequence
task modules:check SERVICE=sequence
task service-release:check SERVICE=sequence VERSION=v0.2.0 MODE=candidate
```

`modules:prepare` regenerates the candidate checksums through a local module
proxy and writes them to the checked-in `go.sum` files. `modules:check` stays
read-only and re-verifies the contract, the SDK, and an external consumer with
`GOWORK=off`. `MODE=candidate` checks the manifest mapping and the isolated
service build without requiring the tags.

After the contract and SDK tags exist, run the strict check, which also
validates the tags, the recorded history, and a build against the real published
dependencies:

```sh
task service-release:check SERVICE=sequence VERSION=v0.2.0 MODE=published
```
