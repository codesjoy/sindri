# Docker Compose deployment

The default stack is self-contained: PostgreSQL, migrations, sequence, an OpenTelemetry Collector, Prometheus, Tempo, and Grafana run in one Compose project.

From the repository root:

```sh
cp deploy/docker/.env.example deploy/docker/.env
docker compose -f deploy/docker/compose.yaml up --build -d
```

Sequence gRPC is available on `localhost:19010`; Grafana is available on `http://localhost:3000`.
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
