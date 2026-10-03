# Changelog

## 2026-08


### Bug fixes

- **sequence:** Harden route convergence and recovery


### Features

- **sequence:** Add sequence service

- **sequence:** Add route-aware client

- **deploy:** Add sequence Docker stack

- **sequence:** Add adaptive range prefetch

- **sequence:** Evict idle allocator key state

- **sequence:** Bound allocator memory usage

- **release:** Map service versions to tested modules


### Refactor

- **sequence:** Separate publishable modules **BREAKING**

## 2026-10


### Bug fixes

- **sequence:** Enforce allocation lease boundaries

- **sequence:** Bound clock uncertainty and recorder history

- **release:** Accept backfilled legacy service tags


### Features

- **sequence:** Add HA ownership and restructure assembly **BREAKING**

- **sequence:** Compact ownership reads and drop the outbox

- **sequence:** Add bounded allocation and partial batch retries

- **sequence:** Rebuild ownership around slot epochs and handoffs **BREAKING**
