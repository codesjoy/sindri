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

| Service | Capability | Contract | Usage | Deployment |
| --- | --- | --- | --- | --- |
| Sequence | Distributed, per-key monotonic ID allocation | [SequenceGenerator v1](api/sindri/sequence/v1/sequence.proto) | [Quick start](docs/sequence.md) | [Docker Compose](deploy/docker/README.md) |

Sequence is currently the only service in the repository. New services follow
the same independent ownership and deployment model.

## Documentation

- [Sequence quick start](docs/sequence.md): deploy the local stack, publish a
  route, and call the Sequence RPCs.
- [Docker deployment guide](deploy/docker/README.md): configure databases,
  external dependencies, observability, and runtime resources.
- [Engineering standards](docs/engineering-standards.md): repository structure,
  service boundaries, testing, and implementation rules.
- [Module release process](docs/module-release.md): publish contracts, clients,
  and deployable service releases.
- [Changelog](CHANGELOG.md): repository-level changes grouped by month.

## License

Sindri is licensed under the [Apache License 2.0](LICENSE).
