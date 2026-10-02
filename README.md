# Sindri

Sindri maintains small, general-purpose infrastructure services that can be
deployed independently and consumed through stable APIs or RPCs. Each service
owns a focused capability and its runtime, contracts, persistence, deployment
assets, and release lifecycle.

## Design principles

- **Focused:** each service solves one bounded infrastructure problem.
- **Independent:** services can be built, deployed, scaled, and released separately.
- **API-first:** capabilities are exposed through versioned Protocol Buffer contracts.
- **Reusable:** services provide application-neutral building blocks rather than
  product-specific workflows.

## Services

| Service | Capability | Contract | Usage | Architecture | Deployment |
| --- | --- | --- | --- | --- | --- |
| Sequence | Distributed, per-key monotonic ID allocation | [SequenceGenerator v1](api/sindri/sequence/v1/sequence.proto) | [Quick start](docs/sequence.md) | [HA architecture](docs/sequence-ha-architecture.md) | [Docker Compose](deploy/docker/README.md) |

Sequence is currently the only service in the repository. New services follow
the same independent ownership and deployment model.

## Documentation

- [Sequence quick start](docs/sequence.md): deploy the local stack, watch the
  directory place itself, and call the Sequence RPCs.
- [Sequence HA architecture](docs/sequence-ha-architecture.md): the safety
  invariants, the slot-epoch and instance-lease protocol, handoff and crash
  recovery, storage, configuration bounds, and observability.
- [Docker deployment guide](deploy/docker/README.md): configure databases,
  external dependencies, observability, runtime resources, the empty-database
  baseline, and the release order.
- [Engineering standards](docs/engineering-standards.md): repository structure,
  service boundaries, testing, and implementation rules.
- [Module release process](docs/module-release.md): publish contracts, clients,
  and deployable service releases.
- [Changelog](CHANGELOG.md): repository-level changes grouped by month.

## Development

Development commands run through [Task](https://taskfile.dev/) v3. Install the
pinned tools and Git hooks, then list every repository command:

```sh
task setup
task --list
```

`task verify` runs the complete merge gate: formatting, static analysis,
Protocol Buffer lint, license headers, unit and race-enabled tests, module
builds, and publishable-module isolation; CI owns this gate. During local
iteration, run the smallest checks that cover the change with
`task test:package PACKAGE=...`, `task test:package:race PACKAGE=...`, or
`task test:service SERVICE=...` instead of the full gate.
`Taskfile.yml` is the source of truth for build, test, lint, contract, and
release commands; generated files are never edited by hand. Pinned tool
installation supports macOS and Linux; on Git Bash the tasks work, but the
pinned tools must be provided separately.

## License

Sindri is licensed under the [Apache License 2.0](LICENSE).
