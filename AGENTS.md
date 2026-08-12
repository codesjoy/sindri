# Repository Guidelines

## Project Structure & Module Organization

`cmd/sequence/main.go` starts the service with `configs/sequence.yaml`. Code under `internal/sequence/` follows `service -> biz <- data`; `app` owns Wire assembly, `conf` configuration, and `task` background work. Reusable APIs belong in `pkg/`; shared test helpers belong in `internal/pkg/tests`. Protocols are in `api/skuld/`; generated Go contracts are in the separate `gen/go` module. Migrations are split by dialect under `migrations/sequence/{postgres,mysql}`. Keep package tests beside code, component tests in `internal/sequence/tests`, and integration tests in `tests/`.

## Build, Test, and Development Commands

- `make build`: compile all Go packages.
- `make test`: run the default unit and component test suite (`go test ./...`).
- `go test -tags=integration ./tests/...`: run PostgreSQL/MySQL contract tests; Docker must be available.
- `make go-lint`: run the configured `golangci-lint` checks.
- `make go-fix`: apply `gofumpt`, `goimports`, `golines`, and safe lint fixes.
- `make proto`: regenerate contracts with Buf and tidy `gen/go`.
- `cd internal/sequence/app && ../../../scripts/generate-wire.sh`: refresh Wire output after changing providers.
- `SKULD_SEQUENCE_DSN=... SKULD_SEQUENCE_NODE_ID=... go run ./cmd/sequence`: run the service locally.

## Coding Style & Naming Conventions

Use tabs and standard Go formatting; run `make go-fix` before review. Package names are short lowercase nouns, exported identifiers require GoDoc, and comments are English. Handlers translate protocols, `biz` defines behavior and repository interfaces, and `data` implements persistence. Never hand-edit `*.pb.go` or `wire_gen.go`. Name migrations `YYYYMMDDHHMMSS_description.sql` with Up/Down sections.

## Testing Guidelines

Name tests `Test<Behavior>` in `*_test.go`. Prefer `testify/require` for prerequisites and `testify/assert` for independent checks. Use SQLite only where behavior is not database-specific; validate PostgreSQL/MySQL semantics with testcontainers. Add focused tests in the changed package and component or integration coverage when wiring, RPC contracts, or persistence behavior changes.

## Commit & Pull Request Guidelines

The repository has no commit history yet; follow `.gitlint` Conventional Commit rules: `feat(sequence): add route refresh`, with a lowercase subject, no trailing period, and at most 72 characters. Install hooks with `make hooks.install`. Pull requests should explain behavior and architecture impact, link the issue, identify config/schema/API changes, and list commands run. Include generated files and both dialect migrations when their sources change; screenshots are only relevant for rendered documentation or UI changes.

## Security & Configuration

Do not commit credentials or concrete DSNs. Supply secrets through environment expansion in `configs/sequence.yaml`. Keep database ownership checks intact and avoid logging raw database or downstream errors.
