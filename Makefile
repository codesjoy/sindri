SHELL := /bin/bash
GO := go

SERVICE ?= sequence
PROFILE ?= service
MODULES := . $(shell find gen/go pkg -name go.mod -type f -print 2>/dev/null | sed 's|/go.mod$$||' | sort)
GOCACHE ?= /tmp/sindri-gocache
GOLANGCI_LINT_CACHE ?= /tmp/sindri-golangci-cache
SEQUENCE_SYSTEM_IMAGE ?= skuld-sequence-system:local
SEQUENCE_TEST_DIALECTS ?= postgres,mysql

.PHONY: scaffold-service scaffold-test release-test proto proto-lint proto-breaking-check wire \
	fmt-check test test-race build modules-check release-check \
	build-sequence-test-image test-sequence-integration test-sequence-chaos \
	go-lint go-fix verify hooks.install

scaffold-service: ## Create SERVICE with PROFILE=service|contract|client
	@test -n "$(SERVICE)" || { echo "SERVICE is required" >&2; exit 2; }
	./scripts/scaffold-service.sh "$(SERVICE)" "$(PROFILE)"

scaffold-test: ## Verify scaffold validation and rollback behavior
	./scripts/test-scaffold-service.sh

release-test: ## Verify optional module release graph discovery
	./scripts/test-module-release.sh

proto: ## Generate contracts for SERVICE and tidy its generated module
	@test -f "gen/go/$(SERVICE)/go.mod" || { echo "service $(SERVICE) has no generated-contract module" >&2; exit 2; }
	cd api && buf generate --path "sindri/$(SERVICE)"
	cd "gen/go/$(SERVICE)" && GOWORK=off $(GO) mod tidy

proto-lint: ## Lint and build all Protocol Buffer modules
	cd api && buf lint
	cd api && buf build >/dev/null

proto-breaking-check: ## Reject breaking Proto changes against main
	buf breaking api --against '.git#branch=main,subdir=api'

wire: ## Regenerate Wire output for SERVICE
	@test -d "internal/$(SERVICE)/app" || { echo "unknown service: $(SERVICE)" >&2; exit 2; }
	cd "internal/$(SERVICE)/app" && GOCACHE=$(GOCACHE) sh ../../../scripts/generate-wire.sh

go-lint: ## Run golangci-lint once per module
	@set -e; for module in $(MODULES); do \
		echo "==> lint $$module"; \
		(cd "$$module" && GOCACHE=$(GOCACHE) GOLANGCI_LINT_CACHE=$(GOLANGCI_LINT_CACHE) golangci-lint run ./...); \
	done

go-fix: ## Apply Go formatting and safe lint fixes once per module
	gofumpt -w cmd internal pkg tests
	goimports -w cmd internal pkg tests
	golines -w cmd internal pkg tests
	@set -e; for module in $(MODULES); do \
		(cd "$$module" && GOCACHE=$(GOCACHE) GOLANGCI_LINT_CACHE=$(GOLANGCI_LINT_CACHE) golangci-lint run --fix ./...); \
	done

fmt-check: ## Verify Go formatting without rewriting source
	@test -z "$$(gofmt -l $$(find cmd internal pkg tests -name '*.go' -type f -print))" || { echo 'gofmt: unformatted Go files' >&2; exit 1; }

test: ## Run unit and component tests in every workspace module
	@set -e; for module in $(MODULES); do \
		echo "==> test $$module"; \
		(cd "$$module" && GOCACHE=$(GOCACHE) $(GO) test ./...); \
	done

test-race: ## Run race-enabled tests in every workspace module
	@set -e; for module in $(MODULES); do \
		echo "==> race $$module"; \
		(cd "$$module" && GOCACHE=$(GOCACHE) $(GO) test -race ./...); \
	done

build: ## Build every workspace module
	@set -e; for module in $(MODULES); do \
		echo "==> build $$module"; \
		(cd "$$module" && GOCACHE=$(GOCACHE) $(GO) build ./...); \
	done

modules-check: ## Verify all publishable modules without workspace replacement
	./scripts/check-module-release.sh "$(SERVICE)"

release-check: proto-lint modules-check ## Validate SERVICE before creating module tags

build-sequence-test-image: ## Build the real Sequence process image used by system tests
	mkdir -p artifacts/sequence
	GOCACHE=$(GOCACHE) CGO_ENABLED=0 GOOS=linux $(GO) build -trimpath -o artifacts/sequence/sequence ./cmd/sequence
	docker build --progress=plain -f tests/sequence/testdata/Dockerfile -t $(SEQUENCE_SYSTEM_IMAGE) .

test-sequence-integration: build-sequence-test-image ## Run Sequence store and E2E tests
	GOCACHE=$(GOCACHE) SKULD_SEQUENCE_TEST_DIALECTS=$(SEQUENCE_TEST_DIALECTS) SKULD_SEQUENCE_TEST_IMAGE=$(SEQUENCE_SYSTEM_IMAGE) $(GO) test -tags=integration -count=1 -timeout=20m ./tests/sequence/...

test-sequence-chaos: build-sequence-test-image ## Run deterministic Sequence system and chaos tests
	GOCACHE=$(GOCACHE) SKULD_SEQUENCE_TEST_DIALECTS=$(SEQUENCE_TEST_DIALECTS) SKULD_SEQUENCE_TEST_IMAGE=$(SEQUENCE_SYSTEM_IMAGE) $(GO) test -tags='integration chaos' -count=1 -timeout=40m ./tests/sequence/...

verify: fmt-check proto-lint scaffold-test release-test test test-race build modules-check ## Run the complete merge gate

hooks.install: ## Install pre-commit and commit-msg hooks
	@if ! command -v pre-commit >/dev/null 2>&1; then \
		echo "pre-commit not found, installing via pipx..."; \
		pipx install pre-commit; \
	fi
	pre-commit install --install-hooks
