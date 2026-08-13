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

## Service release manifests

Deployable services remain in the root Go module but have independent Git
release markers named `service/<service>/vX.Y.Z`. A service tag is not a Go
module tag and does not make `cmd/<service>` or `internal/<service>` importable.

Service, contract, and client versions evolve independently. Their authoritative
compatibility mapping lives in `releases/services/<service>.yaml`:

```yaml
service: sequence
releases:
  - version: v0.1.0
    contract: gen/go/sequence/v0.1.0
    tested_clients:
      - pkg/sequence/v0.1.0
```

`contract` is optional and `tested_clients` may be empty. A client requires a
contract. The contract assigned to a service version is immutable; additional
exact client versions may be appended after the service is released, but an
existing tested client must not be removed.

Run `make service-release-check SERVICE=<service> VERSION=<version>` before
creating a service tag. The check validates referenced tags and module
dependencies, rejects unpublished module source drift, and tests the service
with `GOWORK=off` after removing root-module local replacements.

## Release order

Publish only nested modules that exist:

1. Tag `gen/go/<service>/vX.Y.Z` when generated contracts are independently used.
2. Wait until that tag resolves remotely, then update and tag a dependent
   `pkg/<name>/vX.Y.Z`.

Services and `internal/pkg` are part of the root module. Do not create a service
Go module or an `internal/pkg/...` tag. Create the service Git release marker
after all modules referenced by its manifest have been published and verified.

For Sequence v0.1.0, run `make proto SERVICE=sequence`, `make proto-lint`,
`make modules-check SERVICE=sequence`,
`make service-release-check SERVICE=sequence VERSION=v0.1.0`, then publish in
this order:

```text
gen/go/sequence/v0.1.0
pkg/sequence/v0.1.0
service/sequence/v0.1.0
```

After publishing the SDK, verify a temporary repository-external module can run
`go get github.com/codesjoy/sindri/pkg/sequence@v0.1.0` and compile a minimal
import. Publishing remains an explicit manual operation; repository checks do not
push or create tags. After publishing, run `make chglog` or
`make chglog MONTH=YYYY-MM` and commit the updated repository changelog at
`CHANGELOG.md`. The changelog is a monthly repository summary and is not release
evidence for a service or module. Module tags and service tags remain the
release evidence for consumers and deployable services.

Create an annotated service tag whose message repeats the manifest mapping:

```text
release service/sequence v0.1.0

Contract: gen/go/sequence/v0.1.0
Tested-Client: pkg/sequence/v0.1.0
```

A service-only patch can reuse the existing contract and tested clients. A
client-only patch needs no new service tag: publish the client, verify it against
the existing service, and append its exact tag to that service release entry.
Compatible RPC changes publish contract, then client, then service. Breaking RPC
changes bump each affected artifact independently; equal version numbers do not
imply compatibility.
