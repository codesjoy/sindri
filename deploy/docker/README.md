# Docker Compose deployment

The default stack is self-contained: PostgreSQL, migrations, sequence, an OpenTelemetry Collector, Prometheus, Tempo, and Grafana run in one Compose project.

From the repository root:

```sh
cp deploy/docker/.env.example deploy/docker/.env
docker compose -f deploy/docker/compose.yaml up --build -d
```

Sequence gRPC is available on `localhost:19010`; Grafana is available on `http://localhost:3000`.
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
