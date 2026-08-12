# Engineering Standards

This document defines the engineering rules for this repository. Product-specific
security domains, resource ownership, service contracts, and workflows belong in
the relevant architecture documents. Repository configuration and `go.mod` files
are the source of truth for dependency versions.

## 1. Technology and repository layout

The services use Go, Yggdrasil, Protocol Buffers, GORM, Wire, PostgreSQL, and the
repository's configured observability and Redis integrations. Use the existing
toolchain and shared basic packages for infrastructure primitives; do not create
business-neutral wrappers that hide capabilities or become a second source of
truth.

```text
api/             Protocol Buffer and API definitions
gen/             Generated contracts; never edit by hand
cmd/             Executable entry points (for example, cmd/sequence)
internal/        Service-private implementation
pkg/             Reusable code with no business-resource ownership
migrations/      Database-owner migrations, split by dialect
configs/         Runtime configuration
deploy/          Deployment and infrastructure definitions
tests/           Repository-level integration, end-to-end, and security tests
docs/            Architecture and engineering documentation
```

- Generated code MUST be reproducible from the repository toolchain. Regeneration
  MUST be followed by a clean-tree or generated-diff check. Never hand-edit
  `*.pb.go` or `wire_gen.go`.
- A service's `internal/<service>` package MUST NOT be imported by another service.
  Cross-service calls use generated private protocol clients.
- Name paths by content or responsibility. Do not use phase, milestone, or version
  labels such as `step1`, `phase2`, or `v2` in directory and file names.
- `pkg/` and shared packages may contain protocols, cryptographic or event
  infrastructure, and real infrastructure adapters, but MUST NOT own product
  resources, domain state machines, or copied cross-service facts.

## 2. Ownership and data boundaries

- Each business resource MUST have exactly one source-of-truth service and one
  database owner.
- A service MUST NOT read another service's database, tables, migrations, or ORM
  models. Reads across services use a private API, a bounded cache that falls back
  to that API, an event-built read projection, or an explicitly approved offline
  pipeline.
- Relational databases contain ordinary physical tables only. Do not add foreign
  keys, ordinary or materialized views, user triggers, stored procedures, rules, or
  shared write transactions between database owners.

## 3. Go service architecture

Each process starts at `cmd/<service>/main.go`; Yggdrasil Runtime owns configuration,
RPC/REST servers, and background-task lifecycle.

| Package | Responsibility | Boundary |
| --- | --- | --- |
| `app` | Wire assembly, lifecycle, and registration | MUST NOT contain domain invariants |
| `conf` | Runtime decoding, defaults, and validation | MUST NOT depend on `app` |
| `biz` | Aggregates, use cases, repository interfaces, and domain events | MUST NOT depend on transport or persistence implementations |
| `data` | Database repositories, external clients, and transaction adapters | MUST NOT expose writes that bypass use cases |
| `service` | RPC/REST handlers, resource-name conversion, and error mapping | MUST NOT build non-parameterized SQL |
| `task`/`consumer` | Background and event-driven adapters | MUST call business interfaces, not persistence directly |

The dependency direction is `service -> biz <- data`, assembled by `app`.

- Handlers perform protocol validation, resource-name conversion, use-case calls,
  and error mapping only.
- Open transactions only around the smallest write path that requires atomicity.
  A repository may include business and outbox writes in one transaction. Queries
  and handlers MUST NOT start or manage business transactions.
- Collection APIs and private read protocols MUST provide bounded batch reads. A
  response MUST NOT be assembled with one remote call per item. Remote I/O in a
  loop, recursive traversal, or per-item helper is forbidden; loops may only do
  in-memory collection, validation, deduplication, mapping, or assembly.
- For a collection request, I/O count MUST be independent of result count: one
  primary query plus at most one batch query per explicitly requested expansion or
  dependency type.
- Incomplete RPC methods MUST NOT be registered. Generated
  `Unimplemented...Server` types support interface evolution only; they are not a
  substitute for an implementation.
- Startup MUST validate database connectivity and identity, secret references,
  issuers, internal-service credentials, and any declared projection or cache
  readiness.

### Configuration and Wire

Each implementation package (`data`, `service`, `task`, or `consumer`) defines the
smallest configuration type it needs. The service `conf.Config` is the immutable
composition root: it decodes, applies defaults, validates modules, and validates
cross-module invariants. Constructors receive their package config or explicit
fields, never the whole `*conf.Config`, and must not mutate loaded configuration.

The allowed dependency direction is:

```text
app -> conf -> implementation configuration
app -> data/service/task/consumer -> biz
```

Use `wire.FieldsOf` to expose fields from an already loaded root configuration;
use `wire.Struct` only to construct a new structure from dependencies.

```go
type Config struct {
	Database xgorm.Config `mapstructure:"database"`
	Ticker   task.Config  `mapstructure:"ticker"`
}

var configSet = wire.NewSet(
	wire.FieldsOf(new(*conf.Config), "Database", "Ticker"),
)
```

Providers MUST be small and accept only their own configuration or required
fields. Shared infrastructure constructors live in shared packages and are used
directly by Wire; do not copy per-service wrappers. Keep resource construction
separate from bundle assembly. Backend adapters expose a backend-independent
interface and bind the concrete implementation with `wire.Bind`. Runtime logging
is obtained from `rt.Logger()`; it MUST NOT be reintroduced as a nil-checking
provider or an unrelated Wire dependency.

### Code comments

Comments MUST be in English and explain why or a non-obvious constraint. Do not
repeat the code, cite documentation paths, or include external links.

## 4. Errors and security

- Use stable gRPC status codes. Add a stable reason only for a domain conflict
  that callers cannot recognize from a standard code. Do not wrap standard
  validation, name, field-mask, missing-resource, invalid-state, or authentication
  failures in an extra reason.
- Map unique violations to `ALREADY_EXISTS`, missing resources to `NOT_FOUND`,
  invalid state to `FAILED_PRECONDITION`, and etag conflicts to `ABORTED`.
- Authentication errors MUST NOT reveal account existence, subject state, or the
  specific credential failure.
- Database, secret, and downstream errors MUST be logged with appropriate context
  but MUST NOT be returned verbatim to external callers. Never commit credentials
  or concrete DSNs; use environment expansion in configuration.

## 5. Database migrations

Each database owner has an independent migration directory, DSN, and schema
version table. Name migrations
`YYYYMMDDHHMMSS_description.sql` and include explicit `Up` and `Down` sections.

## 6. Testing and release gates

Release gates MUST NOT be satisfied by skipping tests, registering empty handlers,
or sharing an uncontrolled temporary database.

- Put unit tests beside the package under test. Put service component tests in
  `internal/<service>/tests`; put repository-level integration, end-to-end, and
  security tests in `tests/`.
- Component tests should drive the public RPC through the complete service,
  use-case, repository, and publisher path, checking wiring, contracts, event
  order, and persistence round trips without repeating lower-layer unit tests.
- Unit tests may use in-memory SQLite and miniredis only where behavior is not
  database-specific. Repository production behavior uses PostgreSQL.
- PostgreSQL/MySQL-specific behavior (for example, UUID types, `timestamptz`,
  `ON CONFLICT ... RETURNING`, JSONB, partial indexes, or advisory locks) MUST be
  covered with real databases through testcontainers in integration tests.
- Integration and end-to-end tests MUST start real dependencies with
  testcontainers-go; in-memory substitutes are not sufficient.
- Use `testify/require` for prerequisites and `testify/assert` for independent
  checks. New tests should follow `Test<Behavior>` naming.
- Shared test helpers belong in `internal/pkg/tests`; do not duplicate them across
  service test packages.

Before review, run the repository's configured build, test, lint, formatting, and
protocol-generation checks (normally `make build`, `make test`, `make go-lint`,
`make go-fix`, and `make proto` as applicable).
