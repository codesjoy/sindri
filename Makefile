SHELL := /bin/bash
GO := go

GOCACHE ?= /tmp/skuld-gocache
GOLANGCI_LINT_CACHE ?= /tmp/skuld-golangci-cache
SEQUENCE_SYSTEM_IMAGE ?= skuld-sequence-system:local
SEQUENCE_TEST_DIALECTS ?= postgres,mysql

.PHONY: proto proto-lint proto-breaking-check \
	fmt-check test test-race build-sequence-test-image test-sequence-integration test-sequence-chaos build verify \
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
	GOCACHE=$(GOCACHE) GOLANGCI_LINT_CACHE=$(GOLANGCI_LINT_CACHE) golangci-lint run ./...

go-fix: ## Run golangci-lint
	gofumpt -w .
	goimports -w .
	golines -w .
	golangci-lint run --fix ./...

fmt-check: ## Verify Go formatting without rewriting source
	@test -z "$$(gofmt -l $$(find cmd internal pkg -name '*.go' -type f -print))" || { echo 'gofmt: unformatted Go files' >&2; exit 1; }

test: ## Run contract and retained pure-algorithm tests
	$(GO) test ./...

test-race: ## Run race-enabled foundation and contract tests
	$(GO) test -race ./internal/shared/... ./tools/... ./tests/...

build-sequence-test-image: ## Build the real sequence process image used by system tests
	mkdir -p artifacts/sequence
	CGO_ENABLED=0 GOOS=linux $(GO) build -trimpath -o artifacts/sequence/sequence ./cmd/sequence
	docker build --progress=plain -f tests/sequence/testdata/Dockerfile -t $(SEQUENCE_SYSTEM_IMAGE) .

test-sequence-integration: build-sequence-test-image ## Run sequence store, E2E, and short failover tests
	SKULD_SEQUENCE_TEST_DIALECTS=$(SEQUENCE_TEST_DIALECTS) SKULD_SEQUENCE_TEST_IMAGE=$(SEQUENCE_SYSTEM_IMAGE) $(GO) test -tags=integration -count=1 -timeout=20m ./tests/sequence/...

test-sequence-chaos: build-sequence-test-image ## Run all deterministic sequence system and chaos tests
	SKULD_SEQUENCE_TEST_DIALECTS=$(SEQUENCE_TEST_DIALECTS) SKULD_SEQUENCE_TEST_IMAGE=$(SEQUENCE_SYSTEM_IMAGE) $(GO) test -tags='integration chaos' -count=1 -timeout=40m ./tests/sequence/...

build: ## Compile all retained packages and tools; no business binaries are produced
	$(GO) build ./...


verify: fmt-check test test-race build  ## Run the complete clean-slate/foundation merge gate

hooks.install: ## Install pre-commit git hooks (pre-commit and commit-msg)
	@if ! command -v pre-commit >/dev/null 2>&1; then \
		echo "pre-commit not found, installing via pipx..."; \
		pipx install pre-commit; \
	fi
	pre-commit install --install-hooks
