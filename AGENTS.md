# Sindri Agent Instructions

These instructions are the execution overlay for coding agents working in this
repository. Read the detailed, self-contained engineering rules in
[`docs/engineering-standards.md`](docs/engineering-standards.md) before making
non-trivial changes. When a rule is not summarized here, follow that document.

## Operating Rules

- Inspect the relevant implementation, tests, configuration, migrations, and
  generated-file boundaries before editing.
- Preserve unrelated user changes in the worktree. Never reset, restore, or
  overwrite files outside the requested scope.
- Keep changes narrowly scoped to the owning service, module, or package, and
  reuse the existing local interfaces and helpers.
- Use `apply_patch` for manual edits. Do not use scripts or shell redirection to
  rewrite source files.
- Never hand-edit `*.pb.go` or `wire_gen.go`; change their inputs and regenerate
  them with the repository tasks.
- Do not expose secrets, credentials, DSNs, tokens, authorization headers, or
  sensitive payloads in code, logs, tests, documentation, or command output.
- Report changed files, verification performed, and any remaining limitation in
  the final response.

## Repository Layout and Ownership

Sindri keeps every service process in the root `github.com/codesjoy/sindri` Go
module. A service adds an independent `gen/go/<service>` module only when its
generated contracts must be versioned for consumers, and only selected packages
under `pkg/` are independent modules. Sequence currently has both.

```text
api/sindri/<service>/   Service-owned Protocol Buffer and reason definitions
cmd/<service>/          Root-module process entry point and Wire assembly
configs/                Runtime configuration
gen/go/<service>/       Independently published generated-contract Go module
internal/<service>/     Root-module private service implementation
internal/pkg/           Root-module infrastructure and shared test helpers
migrations/<service>/   Service-owned database migrations
pkg/<name>/             Public package; an independent module only when required
releases/services/      Service release manifests
tests/<service>/        Service integration and system tests
```

- A service owns its contracts, runtime, persistence, deployment assets, and
  release lifecycle. Never import another service's `internal` packages; cross-
  service calls use generated protocol clients instead.
- Sequence starts at `cmd/sequence/main.go` with `configs/sequence.yaml`. The
  command directory is the only composition root: `main.go` is the process entry
  point and `wire.go` owns Wire assembly, lifecycle registration, and bundle
  construction.
- `internal/pkg` is shared inside the root module only. Do not copy shared
  infrastructure into a service directory or let a service reach into another
  service's implementation.
- Every publishable `gen/go/<service>` or independent `pkg/<name>` module MUST
  pass `GOWORK=off` validation without `replace` directives or bootstrap
  versions. The root application module may replace nested modules with local
  paths for development.
- Name paths by content or responsibility. Do not use phase, milestone, or
  version labels such as `step1`, `phase2`, or `v2` in directory and file names.
- Database migrations and integration tests live under `migrations/<service>`
  and `tests/<service>`. Name migrations `YYYYMMDDHHMMSS_description.sql` with
  explicit `Up` and `Down` sections.

## Architecture Boundaries

Each process starts at `cmd/<service>/main.go`; Yggdrasil Runtime owns
configuration, RPC/REST servers, and background-task lifecycle. The private code
domain follows the dependency direction `service -> biz <- data`, assembled by
`cmd/<service>`, with `task` and `consumer` as background entry adapters.

```text
cmd/<service>   Process entry point, Wire assembly, lifecycle, and registration
conf            Runtime decoding, defaults, and cross-package validation
biz             Aggregates, use cases, repository interfaces, and domain events
data            Repositories, external clients, and transaction adapters
service         RPC/REST handlers, resource-name conversion, and error mapping
task/consumer   Background and event-driven adapters calling business interfaces
```

- `cmd/<service>` MUST NOT contain domain invariants, SQL, or business rules;
  `biz` MUST NOT depend on transport or persistence implementations; `data` MUST
  NOT expose writes that bypass use cases.
- Each implementation package owns the configuration section it reads: the
  type, its `mapstructure` and `default` tags, and its `Validate`. `conf`
  composes those sections and validates only cross-package invariants.
  Constructors receive their package config or explicit fields, never the whole
  `*conf.Config`, and must not mutate loaded configuration.
- Handlers perform protocol validation, resource-name conversion, use-case
  calls, and error mapping only. Open transactions only around the smallest
  write path that requires atomicity.
- Collection APIs and private read protocols MUST provide bounded batch reads.
  Remote I/O in a loop, recursive traversal, or per-item helper calls are
  forbidden.
- Register an RPC method only when it is implemented, and validate database
  connectivity, identity, secret references, issuers, credentials, and declared
  projection or cache readiness at startup.

## Verification Commands

Task v3 is the only supported command entry point; `Taskfile.yml` is the source
of truth for command names. The complete merge gate is CI-owned: continuous
integration runs `task verify`, which combines formatting, static analysis,
Protocol Buffer lint, license headers, all-module unit and race tests, builds,
and the publishable-module check. Narrowing a local run to the packages a change
affects is not skipping coverage; hiding, weakening, or ignoring failures is.

Local iteration defaults to targeted checks. Choose the smallest row that covers
the change:

| Change surface | Default local checks |
| --- | --- |
| Single Go package | `task test:package PACKAGE=./internal/<service>/<package>/...` |
| Concurrency, locks, goroutines, or channels | `task test:package:race PACKAGE=./internal/<service>/<package>/...` |
| Multiple packages or an interface inside one service | `task test:service SERVICE=<service>` |
| A `pkg/<name>` module | `task test:package MODULE=./pkg/<name> PACKAGE=./...` |
| Shared `internal/pkg` or cross-package exported interfaces | `task test:package PACKAGE=./...` (whole root module) |
| Protocol Buffer sources | `task proto SERVICE=<service>`, then `task proto:lint`, then service tests |
| Wire or dependency-graph changes | `task wire SERVICE=<service>`, then service tests |
| Go files touched by any change | `task fmt:check` |
| Docs or comments only | No Go test run is required |

Escalate to the full gate locally only when the user asks for it, when CI is
unavailable, or when the change itself touches the gate or module boundaries
(for example `Taskfile.yml`, linters, `go.mod`, `go.work`, or release
manifests). Otherwise leave `task verify` to CI and say so in the final summary.

```sh
task setup          # install pinned tools and Git hooks
task fmt:check      # verify formatting without rewriting files
task go:lint        # static analysis for every module (part of task verify)
task go:fix         # apply formatting and safe lint fixes
task test           # unit and component tests in every workspace module (merge gate)
task test:race      # race-enabled tests in every workspace module (merge gate)
task build          # build every workspace module
task modules:check SERVICE=<service>
task service-release:check SERVICE=<service> VERSION=<version>
task verify         # complete merge gate: format, lint, protos, licenses, tests, race, build, modules
```

Additional commands:

```sh
task test:package PACKAGE=./internal/sequence/biz/... [MODULE=.]  # tests for one package or pattern
task test:package:race PACKAGE=<pattern> [MODULE=.]               # race-enabled single-package tests
task test:service SERVICE=sequence                                # unit and component tests for one service
task proto SERVICE=sequence                                       # regenerate and tidy one service's contracts
task proto:lint                                                   # lint and build all Protocol Buffers
task wire SERVICE=sequence                                        # refresh the service's Wire output
task scaffold:service SERVICE=<name> PROFILE=service|contract|client
task test:sequence:integration                                    # PostgreSQL/MySQL contract tests; Docker required
SKULD_SEQUENCE_DSN=... SKULD_SEQUENCE_NODE_ID=... go run ./cmd/sequence
```

Pinned tools install into `bin/` with version stamps under `bin/.versions`;
build caches live under `.cache/`. Git hooks are installed and run by prek.
Tool installation supports macOS and Linux;
on Git Bash the tasks work, but the pinned tools must be provided separately. A
scaffolded service is a compilation-clean skeleton: wiring the Yggdrasil runtime
into `main.go` is the service's first change. `task --list` shows every command.
Keep tasks portable: use native Task features (`deps`, `for`, `env`, `dir`,
`preconditions`) instead of inline POSIX shell wherever Task can express the
same behavior; put unavoidable shell logic in `scripts/` behind a task.

## AI Change Workflow

1. Identify the owning service, module, or package and read its implementation
   and tests.
2. Check configuration, interfaces, migrations, generated files, release
   manifests, and the current worktree status before editing.
3. Implement the smallest coherent change in the correct layer.
4. Add or update regression tests for changed behavior and failure paths.
5. Regenerate Wire or Protocol Buffer output only when its inputs changed.
6. Run the targeted checks from the verification ladder in "Verification
   Commands"; escalate to the full gate only in the cases that section names.
7. Review the diff for unrelated changes, leaked secrets, missing cleanup,
   unbounded I/O, and inconsistent error handling.
8. Summarize the result with file references, the exact verification commands
   run, and any full-gate checks delegated to CI.

If repository behavior, tests, and a requested change disagree, preserve the
existing contract unless the change explicitly updates that contract and its
regression coverage.

## Commit, Release, and Review Rules

- Run `task hooks:install` after checkout. Branch names must be `main`,
  `master`, `develop`, or use
  `(feature|fix|chore|docs|refactor|release|hotfix)/<description>`; detached
  HEAD is not allowed.
- Follow `.gitlint` and the Git and change review rules in
  `docs/engineering-standards.md`. Commit headers use
  `<type>(<scope>): <description>` or `<type>: <description>`, for example
  `feat(sequence): add route refresh`, with a lowercase description, no
  trailing period, and at most 72 characters. Use a scope when one service,
  module, or package owns the change. A body is required for large or mixed
  changes, and breaking changes require both `!` and a `BREAKING CHANGE:` body
  entry.
- Nested module tags include the module directory, for example
  `gen/go/sequence/v0.1.0` and `pkg/sequence/v0.1.0`. Deployable services use
  `service/<service>/vX.Y.Z` tags; these are Git release markers, not Go module
  versions. Record the exact contract and tested clients in
  `releases/services/<service>.yaml` and release in dependency order as
  documented in `docs/module-release.md`.
- Pull requests must explain behavior and architecture impact, identify
  config/schema/API changes, list commands run, and cover generated files,
  compatibility, rollback, and both dialect migrations when applicable.

## Security and Configuration

- Do not commit credentials or concrete DSNs. Supply secrets through
  environment expansion in service configuration.
- Keep database ownership checks intact and avoid logging raw database or
  downstream errors.
- Tests and release gates MUST NOT be satisfied by skipping coverage, hiding
  failures, or sharing an uncontrolled temporary database. Running the targeted
  checks for the affected packages locally is not skipping coverage; CI still
  runs the complete gate.
