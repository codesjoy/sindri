# Go Module Release Process

Sindri uses one root application module for all services and shared internal
packages. Only artifacts that need an independent consumer lifecycle are nested
modules:

```text
github.com/codesjoy/sindri                    services and internal/pkg
github.com/codesjoy/sindri/gen/go/<service>   generated contracts, when present
github.com/codesjoy/sindri/pkg/<name>          independently consumed package, when present
```

Not every service needs a generated-contract module or a public client module.
An independently published client that imports generated contracts requires the
corresponding gen module. Ordinary `pkg/` packages may stay in the root module.

## Workspace and replacements

Go does not apply a dependency module's `replace` directives in a consuming
module. Publishable nested modules therefore require real upstream versions and
must not contain local `replace` directives, `v0.0.0`, or zero pseudo-versions.
The root application module is not an independently consumed library and may
replace its nested gen and SDK dependencies with local paths.

The committed `go.work` and `go.work.sum` make the root module and nested modules
available together during repository development. They do not affect external
users and are not release evidence. Run
`make modules-check SERVICE=<service>` to test the service's independent modules
with `GOWORK=off` and to compile an external SDK consumer when an SDK exists.

## Release order

Publish only nested modules that exist:

1. Tag `gen/go/<service>/vX.Y.Z` when generated contracts are independently used.
2. Wait until that tag resolves remotely, then update and tag a dependent
   `pkg/<name>/vX.Y.Z`.

Services and `internal/pkg` are part of the root module. Do not create
per-service or `internal/pkg/...` module tags.

For Sequence v0.1.0, run `make proto SERVICE=sequence`, `make proto-lint`, and
`make modules-check SERVICE=sequence`, then publish in this order:

```text
gen/go/sequence/v0.1.0
pkg/sequence/v0.1.0
```

After publishing the SDK, verify a temporary repository-external module can run
`go get github.com/codesjoy/sindri/pkg/sequence@v0.1.0` and compile a minimal
import. Publishing remains an explicit manual operation; repository checks do not
push or create tags.
