# Repository Guidelines

## Project Structure & Module Organization

Sindri keeps all service processes in the root `github.com/codesjoy/sindri` Go
module. A service adds an independent `gen/go/<service>` module only when its
generated contracts must be versioned for consumers, and only selected packages
under `pkg/` are independent modules. Sequence has both. It starts at
`cmd/sequence/main.go` with
`configs/sequence.yaml`. Its private code follows
`service -> biz <- data` under `internal/sequence`; `app` owns Wire
assembly, `conf` configuration, and `task` background work.

Protocol sources, including service-owned reasons, live under
`api/sindri/<service>`. Never import another service's `internal` packages.
Repository-wide internal infrastructure and test helpers live in `internal/pkg`
inside the root module. Generated code is never edited by hand. Database
migrations and integration tests live under `migrations/<service>` and
`tests/<service>`.

## Build, Test, and Development Commands

- `make build`: compile every workspace module.
- `make test`: run unit and component tests in every module.
- `make modules-check SERVICE=sequence`: verify publishable modules with
  `GOWORK=off` and without local replacements.
- `make proto SERVICE=sequence`: regenerate and tidy one service's contracts.
- `make proto-lint`: lint and build all Protocol Buffer definitions.
- `make wire SERVICE=sequence`: refresh the service's Wire output.
- `make scaffold-service SERVICE=<name> PROFILE=service|contract|client`: create
  the service directory plus optional contract and SDK modules; `service` is the
  default profile.
- `make test-sequence-integration`: run PostgreSQL/MySQL contract tests; Docker
  must be available.
- `SKULD_SEQUENCE_DSN=... SKULD_SEQUENCE_NODE_ID=... go run ./cmd/sequence`:
  run Sequence locally.

`go.work` and `go.work.sum` are committed so the root module and independent
modules use the same checkout. They are not release evidence: every publishable
`gen/go/<service>` or independent `pkg/<name>` module must also pass
`GOWORK=off`. Publishable `go.mod` files must not contain `replace` directives or
bootstrap versions. The root application module may use local replacements for
its nested modules so `go mod tidy` and container builds do not depend on tags
that have not been published yet.

## Coding Style & Naming Conventions

Use tabs and standard Go formatting; run `make go-fix` before review. Package
names are short lowercase nouns, exported identifiers require GoDoc, and
comments are English. Handlers translate protocols, `biz` defines behavior and
repository interfaces, and `data` implements persistence. Never hand-edit
`*.pb.go` or `wire_gen.go`. Name migrations
`YYYYMMDDHHMMSS_description.sql` with Up/Down sections.

## Testing Guidelines

Name tests `Test<Behavior>` in `*_test.go`. Prefer `testify/require` for
prerequisites and `testify/assert` for independent checks. Use SQLite only where
behavior is not database-specific; validate PostgreSQL/MySQL semantics with
testcontainers. Put package tests beside code, component tests in
`internal/<service>/tests`, and integration tests in `tests/<service>`.

## Commit, Release, and Pull Request Guidelines

Follow `.gitlint` Conventional Commit rules: `feat(sequence): add route refresh`,
with a lowercase subject, no trailing period, and at most 72 characters. Nested
module tags include the module directory, for example
`gen/go/sequence/v0.1.0` and `pkg/sequence/v0.1.0`. The root module contains the
services and shared internal code; it is not tagged per service. Release only
independent modules, in dependency order as documented in
`docs/module-release.md`.

Pull requests should explain behavior and architecture impact, identify
config/schema/API changes, and list commands run. Include generated files and
both dialect migrations when their sources change.

## Security & Configuration

Do not commit credentials or concrete DSNs. Supply secrets through environment
expansion in service configuration. Keep database ownership checks intact and
avoid logging raw database or downstream errors.
