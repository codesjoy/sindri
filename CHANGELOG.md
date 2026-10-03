# Changelog

## 2026-10


### Breaking changes

- **sequence:** Rebuild ownership around slot epochs and handoffs
  RouteSnapshot.nodes is removed and field 2 is reserved, the
  pkg/sequence batch client is replaced by NewClient with explicit
  ErrBatchUnsupported and ErrCountUnsupported failures, and the previous
  migration chain has no in-place upgrade path.

- **sequence:** Add HA ownership and restructure assembly
  app.sequence.allocator/node/ha moved under dataplane,
  app.sequence.control is now controlplane, and stated zero values now take
  their tag defaults instead of being rejected.


### Bug fixes

- **release:** Accept backfilled legacy service tags

- **sequence:** Bound clock uncertainty and recorder history

- **sequence:** Enforce allocation lease boundaries


### Features

- **sequence:** Rebuild ownership around slot epochs and handoffs **BREAKING**

- **sequence:** Add bounded allocation and partial batch retries

- **sequence:** Compact ownership reads and drop the outbox

- **sequence:** Add HA ownership and restructure assembly **BREAKING**

## 2026-09


### Features

- **sequence:** Add batch allocation and adaptive prefetch

## 2026-08


### Breaking changes

- **sequence:** Separate publishable modules
  Go modules and generated contract imports move from
  github.com/codesjoy/skuld to github.com/codesjoy/sindri.


### Bug fixes

- **sequence:** Harden route convergence and recovery


### Features

- **sequence:** Make application name configurable

- **release:** Map service versions to tested modules

- **sequence:** Bound allocator memory usage

- **sequence:** Evict idle allocator key state

- **sequence:** Add adaptive range prefetch

- **deploy:** Add sequence Docker stack

- **sequence:** Add route-aware client

- **sequence:** Add sequence service


### Refactor

- **sequence:** Separate publishable modules **BREAKING**
