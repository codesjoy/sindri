SHELL := /bin/bash
GO := go

GOCACHE ?= /tmp/skuld-gocache

.PHONY: proto proto-lint proto-breaking-check \
	fmt-check test test-race build  verify \
	hooks.install

proto: ## Generate the frozen contract Go contracts
	cd api && buf generate
	cd gen && $(GO) mod tidy

proto-lint: proto-layout-check ## Lint and build the contract Proto modules
	cd api && buf lint
	cd api && buf build >/dev/null

proto-breaking-check: ## Reject breaking Proto contract changes against main
	buf breaking api --against '.git#branch=main,subdir=api'

go-lint: ## Run golangci-lint
	golangci-lint run ./...

go-fix: ## Run golangci-lint
	gofumpt -w .
	goimports -w .
	golines -w .
	golangci-lint run --fix ./...

fmt-check: ## Verify Go formatting without rewriting source
	@test -z "$$(gofmt -l $$(find cmd internal pkg tools tests -name '*.go' -type f -print))" || { echo 'gofmt: unformatted Go files' >&2; exit 1; }

test: ## Run contract and retained pure-algorithm tests
	$(GO) test ./...

test-race: ## Run race-enabled foundation and contract tests
	$(GO) test -race ./internal/shared/... ./tools/... ./tests/...

build: ## Compile all retained packages and tools; no business binaries are produced
	$(GO) build ./...


verify: fmt-check test test-race build  ## Run the complete clean-slate/foundation merge gate

hooks.install: ## Install pre-commit git hooks (pre-commit and commit-msg)
	@if ! command -v pre-commit >/dev/null 2>&1; then \
		echo "pre-commit not found, installing via pipx..."; \
		pipx install pre-commit; \
	fi
	pre-commit install --install-hooks
