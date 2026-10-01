# Engineering Standards

This document defines the engineering rules for this repository. Product-specific
security domains, resource ownership, service contracts, and workflows belong in
the relevant architecture documents. Repository configuration and `go.mod` files
are the source of truth for dependency versions.

The words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are
normative. A change that violates a MUST rule requires an explicit design
decision recorded in the change review.

## 1. Technology and repository layout

The services use Go, Yggdrasil, Protocol Buffers, GORM, Wire, PostgreSQL, and the
repository's configured observability and Redis integrations. Use the existing
toolchain and shared basic packages for infrastructure primitives; do not create
business-neutral wrappers that hide capabilities or become a second source of
truth.

```text
api/sindri/<service>/   Service-owned Protocol Buffer and reason definitions
gen/go/<service>/      Independently published generated-contract Go module
internal/pkg/           Root-module packages shared by Sindri services
pkg/<name>/             Public package; an independent module only when required
cmd/<service>/          Root-module service entry point
internal/<service>/     Root-module private service implementation
configs/                Runtime configuration
migrations/<service>/   Service-owned database migrations
tests/<service>/        Service integration and system tests
docs/                  Architecture and engineering documentation
```

- Generated code MUST be reproducible from the repository toolchain. Regeneration
  MUST be followed by a clean-tree or generated-diff check. Never hand-edit
  `*.pb.go` or `wire_gen.go`.
- A service's `internal/<service>` packages MUST NOT be imported by
  another service. Cross-service calls use generated private protocol clients.
- Every service MUST own `cmd/<service>` and `internal/<service>` in the root
  module. A generated
  contract module is created only when contracts are published, and a public
  client module is created only when an SDK is published; an SDK requires its
  generated-contract module.
- Infrastructure and test helpers genuinely shared by Sindri services belong in
  root-module `internal/pkg`. Go's `internal` visibility prevents repositories
  outside `github.com/codesjoy/sindri` from importing it. Shared code must not be
  copied or hidden in a service directory.
- Independent `gen/go/<service>` and `pkg/<name>` modules MUST NOT contain
  `replace` directives or placeholder versions. The committed `go.work` is for
  repository development only; release validation MUST run with `GOWORK=off`.
  The non-published root application module may replace nested modules with their
  local paths to keep root-module tooling reproducible before tags exist.
- Name paths by content or responsibility. Do not use phase, milestone, or version
  labels such as `step1`, `phase2`, or `v2` in directory and file names.
- `pkg/`, `internal/pkg`, and shared packages may contain protocols, cryptographic or event
  infrastructure, and real infrastructure adapters, but MUST NOT own product
  resources, domain state machines, or copied cross-service facts.

### Repository commands

Task v3 is the repository command runner, and `Taskfile.yml` is the source of
truth for command names and pinned tool versions. `task --list` lists every
development, verification, and release command, and `task setup` installs the
pinned tools into `bin/` and the Git hooks. `task verify` is the complete merge
gate and the only definition of it: formatting, static analysis, Protocol
Buffer lint, license headers, unit and component tests, race tests, module
builds, and the publishable-module check. Continuous integration owns this gate
and invokes `task verify` instead of repeating that list. Local iteration uses
the scoped test tasks in section 6 (`task test:package`,
`task test:package:race`, and `task test:service`) so only the checks a change
can affect run; narrowing a local run MUST NOT hide or ignore a failure. Do not
add ad-hoc shell entry points that duplicate a task.

Pinned tool versions live in `Taskfile.yml`; `scripts/install-tools.sh` installs
and verifies them from the environment Task passes in. Tool installation
supports macOS and Linux; on Git Bash the tasks work, but the pinned tools must
be provided separately. Go and golangci-lint caches default to `.cache/go-build`
and `.cache/golangci-lint`; the `GOCACHE` and `GOLANGCI_LINT_CACHE` environment
variables override them.

Tasks SHOULD prefer native Task semantics (`deps`, `for`, `env`, `dir`,
`preconditions`) over inline POSIX shell, so the same task works on every
supported platform. Inline shell is limited to what Task cannot express, and
multi-step shell logic belongs in `scripts/` behind a task.

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
| `cmd/<service>` | Process entry point, Wire assembly, lifecycle, and registration | MUST NOT contain domain invariants, SQL, or business rules |
| `conf` | Runtime decoding, defaults, and validation | MUST NOT depend on `cmd/<service>` |
| `biz` | Aggregates, use cases, repository interfaces, and domain events | MUST NOT depend on transport or persistence implementations |
| `data` | Database repositories, external clients, and transaction adapters | MUST NOT expose writes that bypass use cases |
| `service` | RPC/REST handlers, resource-name conversion, and error mapping | MUST NOT build non-parameterized SQL |
| `task`/`consumer` | Background and event-driven adapters | MUST call business interfaces, not persistence directly |

The dependency direction is `service -> biz <- data`, assembled by
`cmd/<service>`. `main.go` stays a process entry point: it parses process flags,
names the application, and runs Yggdrasil with a short `compose` function that
loads configuration and calls the Wire injector. `wire.go` carries the providers,
the business bundle, and mode selection, and `wire_gen.go` is generated.

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

Each implementation package (`biz`, `data`, `service`, `task`, or `consumer`)
owns the configuration it reads: the type, its `mapstructure` tags, its
`default` tags, and its `Validate`. The service `conf.Config` is the immutable
composition root: it decodes, composes the package-owned sections, and validates
only the invariants that cross package boundaries. Constructors receive their
package config or explicit fields, never the whole `*conf.Config`, and must not
mutate loaded configuration.

Defaults belong on the fields as `default` tags. The framework's configuration
snapshot applies them after decoding, so a key that was omitted and one stated
as zero are the same thing: both keep the declared default. `SetDefaults` methods
are therefore not part of the pattern, and `Validate` MUST refuse only values
that are wrong at any size -- negative or out-of-range ones -- rather than
zeroes the framework already filled in.

The allowed dependency direction is:

```text
cmd/<service> -> conf -> implementation configuration
cmd/<service> -> data/service/task/consumer -> biz
```

Use `wire.FieldsOf` to expose fields from an already loaded root configuration;
use `wire.Struct` only to construct a new structure from dependencies. Providers
MUST NOT merge one configuration section into another: when a value belongs to a
different section, the package that owns the configuration reads both sections
from the type it is given, and Wire only selects the section providers each role
needs.

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
  `internal/<service>/tests`; put integration, end-to-end, and security
  tests in `tests/<service>`.
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
- Service-local test helpers belong under that service's `internal`. Helpers
  genuinely shared across services belong in `internal/pkg/tests`.

Verification is tiered. Run the smallest tier that covers the change; CI owns
the merge gate.

Tier 0 is the default for every change:

| Change surface | Local checks |
| --- | --- |
| Single Go package | `task test:package PACKAGE=./internal/<service>/<package>/...` |
| Concurrency, locks, goroutines, or channels | `task test:package:race PACKAGE=./internal/<service>/<package>/...` |
| Multiple packages or an interface inside one service | `task test:service SERVICE=<service>` |
| A `pkg/<name>` module | `task test:package MODULE=./pkg/<name> PACKAGE=./...` |
| Shared `internal/pkg` or cross-package exported interfaces | `task test:package PACKAGE=./...` (whole root module) |
| Protocol Buffer sources | `task proto SERVICE=<service>`, `task proto:lint`, then service tests |
| Wire or dependency-graph changes | `task wire SERVICE=<service>`, then service tests |
| Go files touched by any change | `task fmt:check` |
| Docs or comments only | No Go test run is required |

Tier 1 is the merge gate and is owned by CI: `task verify` combines formatting,
static analysis, Protocol Buffer lint, license headers, unit and component tests
for every module, race tests, builds, and the publishable-module check. Run it
locally only when the user asks, when CI is unavailable, or when the change
touches the gate itself or module boundaries (for example `Taskfile.yml`,
linters, `go.mod`, `go.work`, or release manifests). Narrowing local runs is not
skipping coverage; failing or hiding a check is.

Tier 2 covers integration and release surfaces:

```sh
task test:sequence:integration                  # PostgreSQL/MySQL integration tests; Docker required
task test:sequence:chaos                        # deterministic system and chaos tests; Docker required
task service-release:check SERVICE=<service> VERSION=<version>
```

The complete command surface remains:

```sh
task tools:install                              # when pinned tools are missing
task fmt:check                                  # verify formatting without rewriting files
task go:fix                                     # apply formatting and safe lint fixes
task test                                       # unit and component tests in every workspace module (merge gate)
task test:race                                  # race-enabled tests in every workspace module (merge gate)
task build                                      # build every workspace module
task modules:check SERVICE=<service>            # publishable modules with GOWORK=off
task service-release:check SERVICE=<service> VERSION=<version>
task verify                                     # complete merge gate, including go:lint and license:check
```

Use `task proto SERVICE=<service>` after changing Protocol Buffer sources and
`task wire SERVICE=<service>` after changing a service dependency graph.
Generated output MUST be reviewed together with its input change.

## 7. Module release rules

- Nested module tags MUST include the module directory, for example
  `gen/go/sequence/v0.1.0` and `pkg/sequence/v0.1.0`.
- Services remain in the root module but use `service/<service>/vX.Y.Z` Git tags
  as deployment release markers. Service tags are not Go module versions and do
  not make `internal/<service>` independently importable. `internal/pkg` is not
  tagged independently.
- Every service release MUST have a `releases/services/<service>.yaml` entry that
  records its immutable contract tag and exact tested client tags. Tested clients
  MAY be appended after service publication; existing clients MUST NOT be removed.
- Release only independent modules that exist. The dependency order is generated
  contracts first, then a dependent public client, then the service.
  A downstream `go.mod` may reference a version only after that upstream version
  is available from the remote module source.
- A dependency module's `replace` directive is ignored by its consumers. Local
  development replacement belongs in `go.work`, never in a published module.

## 8. Git and change review

Git history is part of the repository's engineering interface. Install the
repository hooks with `task hooks:install`; [prek](https://github.com/j178/prek)
manages the shims. The pre-commit and commit-msg hooks enforce local branch
naming and commit-message policy.

### 8.1 Branch names

- Branch names MUST be `main`, `master`, `develop`, or use one of
  `feature/<description>`, `fix/<description>`, `chore/<description>`,
  `docs/<description>`, `refactor/<description>`, `release/<description>`, or
  `hotfix/<description>`.
- A branch description MUST start with a lowercase letter or digit and may then
  contain lowercase letters, digits, `.`, `_`, `-`, or `/`.
- A detached HEAD MUST NOT be used for changes.

### 8.2 Commit messages

Commit messages MUST use one of the following Conventional Commit types:
`feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`,
`chore`, or `revert`.

The header MUST use `<type>(<scope>): <description>` or
`<type>: <description>`. The type and scope MUST be lowercase.

#### Scope selection

[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/#specification)
defines `scope` as an optional noun describing a section of the codebase.
[Angular's commit message format](https://github.com/angular/angular/blob/main/contributing-docs/commit-message-format.md)
similarly uses the affected package or subsystem. Sindri keeps the field
syntactically optional but applies these selection rules:

- A commit MUST include a scope when exactly one service, module, bounded
  context, or reusable package owns the change, even when the change spans
  multiple layers within that owner. The commit type does not determine whether
  a scope is needed: `test(sequence): ...` and `docs(sequence): ...` are valid.
- A commit MUST omit the scope when no single stable owner exists.
  Repository-wide `build`, `ci`, `docs`, and `chore` changes normally omit it;
  `style`, `test`, and `revert` follow the same ownership test rather than a
  type-specific exception. If a mixed change cannot be split, omit the scope
  and list the affected areas in the body.
- For cross-service workflows, the scope MUST be the service that owns the
  user-facing operation. Omit the scope only when ownership is genuinely shared.
- A scope MUST be one lowercase kebab-case name from a stable service, module,
  or package boundary, such as `sequence`, `release`, or a reusable package
  name. Choose the narrowest stable boundary name; do not encode a file or
  function path.
- A scope MUST NOT be a layer name (`biz`, `data`, `service`), file type, change
  verb (`update`, `cleanup`), ticket or issue identifier, author name, release
  number, or transient project name.

The description MUST start with a lowercase letter, MUST NOT end with a period,
and the complete header MUST NOT exceed 72 characters. Body lines MUST NOT
exceed 100 characters.

A small commit MAY omit the body. A body MUST be present when a commit changes
at least 8 files, adds and deletes at least 200 lines in total, or contains
multiple behavior changes. A required body MUST contain 1 to 4 concise bullet
points.

Breaking changes MUST include `!` in the header and a
`BREAKING CHANGE: <description>` entry in the body. Migration and configuration
changes MUST still be called out explicitly in the review.

### 8.3 Pull requests and review

Pull requests MUST explain the behavior and architecture impact, identify
configuration, schema, and API changes, and list the exact commands run.
Generated files and both dialect migrations MUST be included when their sources
change.

Each change review SHOULD answer:

- What behavior or contract changed?
- Which service, module, or package owns the change?
- What validation and error behavior was added or preserved?
- Are persistence, configuration, generated output, deployment, or release
  files affected?
- Which tests and verification commands were run?
- Are compatibility, rollback, and observability concerns addressed?

The module and service release rules in section 7 remain authoritative for
tags, release manifests, and publication order.

## 9. AI-assisted changes

AI-generated changes follow the same ownership, layering, and review rules as
human changes. Before editing, an agent MUST inspect the relevant package,
tests, configuration, migrations, and generated-file boundaries, and MUST
preserve unrelated changes in the worktree.

Before handing off a change, the agent SHOULD verify:

- The smallest responsible service, module, or package owns the behavior.
- Existing interfaces, helpers, and repository tasks were reused where
  appropriate.
- Validation happens at the trust boundary that owns it, and business
  invariants remain in `biz`.
- Contexts, timeouts, error mapping, idempotency, and cleanup are handled.
- Tests cover success, invalid input, repeated requests, and dependency
  failures relevant to the change.
- Generated files were regenerated with the repository tasks instead of edited
  by hand.
- The Tier 0 checks from section 6 that cover the change pass; run the Tier 1
  gate locally only in the cases section 6 names.
- The final summary names the changed files, the exact verification commands
  run, and any Tier 1 or Tier 2 checks delegated to CI or left as a remaining
  limitation.

When repository behavior and a proposed change conflict, the implementation
MUST follow the existing code and tests unless the change explicitly updates the
affected contract and its regression coverage.
