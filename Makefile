# Copyright 2026 Codesjoy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SHELL := /bin/bash
GO := go
PYTHON ?= python3

SERVICE ?= sequence
PROFILE ?= service
VERSION ?=
MODULES := . $(shell find gen/go pkg -name go.mod -type f -print 2>/dev/null | sed 's|/go.mod$$||' | sort)
GOCACHE ?= /tmp/sindri-gocache
GOLANGCI_LINT_CACHE ?= /tmp/sindri-golangci-cache
SEQUENCE_SYSTEM_IMAGE ?= skuld-sequence-system:local
SEQUENCE_TEST_DIALECTS ?= postgres,mysql

TOOL_DIR := $(CURDIR)/bin
TOOL_STAMP_DIR := $(TOOL_DIR)/.versions
COPYRIGHT_YEAR ?= $(shell date +%Y)
ADDLICENSE_VERSION := v1.2.0
BUF_VERSION := v1.55.1
WIRE_VERSION := v0.7.0
GOLANGCI_LINT_VERSION := v2.12.2
PRE_COMMIT_VERSION := 4.6.1
GIT_CLIFF_VERSION := 2.13.1

ADDLICENSE := $(TOOL_DIR)/addlicense
BUF := $(TOOL_DIR)/buf
WIRE := $(TOOL_DIR)/wire
GOLANGCI_LINT := $(TOOL_DIR)/golangci-lint
PRE_COMMIT := $(TOOL_DIR)/pre-commit-venv/bin/pre-commit
GIT_CLIFF := $(TOOL_DIR)/git-cliff

LICENSE_SCAN_DIRS := cmd internal pkg gen tests tools api deploy releases scripts .github/workflows configs .golangci.yaml .pre-commit-config.yaml cliff.toml
LICENSE_IGNORES := -ignore '**/*.pb.go' -ignore '**/wire_gen.go' -ignore '**/*.sum' \
	-ignore '**/*.mod' -ignore '**/*.work' -ignore '**/*.lock' -ignore '**/*.tmpl' \
	-ignore 'LICENSE'
LICENSE_MANUAL_FILES := Makefile deploy/docker/Dockerfile.go-service deploy/docker/Dockerfile.goose \
	scripts/install-tools.sh \
	scripts/templates/service/config.yaml.tmpl scripts/templates/service/main.go.tmpl \
	scripts/templates/service/pkg.doc.go.tmpl scripts/templates/service/reason.proto.tmpl \
	scripts/templates/service/release.yaml.tmpl \
	scripts/templates/service/service.proto.tmpl scripts/templates/service/workflow.client.yml.tmpl \
	scripts/templates/service/workflow.contract.yml.tmpl scripts/templates/service/workflow.service.yml.tmpl

define require-tool
	@test -x "$(1)" && test -f "$(TOOL_STAMP_DIR)/$(2)-$(3)" || { echo '$(2) $(3) is missing; run make tools.install' >&2; exit 1; }
endef

.PHONY: scaffold-service scaffold-test release-test chglog chglog-init chglog-test tools.install tools.check license license-check \
	proto proto-lint proto-breaking-check wire \
	fmt-check test test-race build modules-check service-release-check release-check \
	build-sequence-test-image test-sequence-integration test-sequence-chaos \
	go-lint go-fix verify hooks.install

tools.install: ## Install pinned development tools into ./bin
	@test -x "$$(command -v $(GO) 2>/dev/null || true)" || { echo 'go is required to install tools' >&2; exit 1; }
	@test -x "$$(command -v $(PYTHON) 2>/dev/null || true)" || { echo 'python3 is required to install pre-commit' >&2; exit 1; }
	@test -x "$$(command -v curl 2>/dev/null || true)" || { echo 'curl is required to install binary tools' >&2; exit 1; }
	@test -x "$$(command -v tar 2>/dev/null || true)" || { echo 'tar is required to install binary tools' >&2; exit 1; }
	TOOL_DIR="$(TOOL_DIR)" TOOL_STAMP_DIR="$(TOOL_STAMP_DIR)" GO_BIN="$(GO)" PYTHON_BIN="$(PYTHON)" \
		ADDLICENSE_VERSION="$(ADDLICENSE_VERSION)" BUF_VERSION="$(BUF_VERSION)" WIRE_VERSION="$(WIRE_VERSION)" \
		GOLANGCI_LINT_VERSION="$(GOLANGCI_LINT_VERSION)" PRE_COMMIT_VERSION="$(PRE_COMMIT_VERSION)" \
		GIT_CLIFF_VERSION="$(GIT_CLIFF_VERSION)" ./scripts/install-tools.sh

tools.check: ## Check pinned development tools in ./bin
	@set -e; missing=0; \
	for pair in \
		"$(ADDLICENSE):$(ADDLICENSE_VERSION)" "$(BUF):$(BUF_VERSION)" "$(WIRE):$(WIRE_VERSION)" \
		"$(GOLANGCI_LINT):$(GOLANGCI_LINT_VERSION)" "$(GIT_CLIFF):$(GIT_CLIFF_VERSION)" \
		"$(PRE_COMMIT):$(PRE_COMMIT_VERSION)"; do \
		binary="$${pair%%:*}"; version="$${pair##*:}"; name="$$(basename "$$binary")"; \
		case "$$binary" in *pre-commit-venv/*) name=pre-commit;; esac; \
		if [ ! -x "$$binary" ] || [ ! -f "$(TOOL_STAMP_DIR)/$$name-$$version" ]; then \
			echo "missing tool or version stamp: $$name $$version (run make tools.install)" >&2; missing=1; \
		fi; \
	done; \
	[ "$$missing" -eq 0 ]

license: ## Add Apache 2.0 headers to supported source files
	$(call require-tool,$(ADDLICENSE),addlicense,$(ADDLICENSE_VERSION))
	$(ADDLICENSE) -l apache -c Codesjoy -y "$(COPYRIGHT_YEAR)" $(LICENSE_IGNORES) $(LICENSE_SCAN_DIRS)

license-check: ## Check Apache 2.0 headers in supported source files
	$(call require-tool,$(ADDLICENSE),addlicense,$(ADDLICENSE_VERSION))
	$(ADDLICENSE) -check -l apache -c Codesjoy -y "$(COPYRIGHT_YEAR)" $(LICENSE_IGNORES) $(LICENSE_SCAN_DIRS)
	@grep -q 'Apache License' LICENSE && grep -q 'Version 2.0' LICENSE || { echo 'LICENSE is not Apache License 2.0' >&2; exit 1; }
	@set -e; for file in $(LICENSE_MANUAL_FILES); do \
		grep -Eq 'Copyright (\{\{YEAR\}\}|[0-9]{4}) Codesjoy' "$$file" || { echo "missing license header: $$file" >&2; exit 1; }; \
		grep -q 'Licensed under the Apache License, Version 2.0' "$$file" || { echo "missing Apache 2.0 header: $$file" >&2; exit 1; }; \
	done

scaffold-service: ## Create SERVICE with PROFILE=service|contract|client
	@test -n "$(SERVICE)" || { echo "SERVICE is required" >&2; exit 2; }
	./scripts/scaffold-service.sh "$(SERVICE)" "$(PROFILE)"

scaffold-test: ## Verify scaffold validation and rollback behavior
	./scripts/test-scaffold-service.sh

release-test: ## Verify optional module release graph discovery
	./scripts/test-module-release.sh
	GOCACHE=$(GOCACHE) $(GO) test ./tools/service-release-check

chglog: ## Update the monthly repository changelog section
	$(call require-tool,$(GIT_CLIFF),git-cliff,$(GIT_CLIFF_VERSION))
	GIT_CLIFF_BIN="$(GIT_CLIFF)" ./scripts/chglog.sh month "$(MONTH)"

chglog-init: ## Rebuild repository changelog from commit months
	$(call require-tool,$(GIT_CLIFF),git-cliff,$(GIT_CLIFF_VERSION))
	GIT_CLIFF_BIN="$(GIT_CLIFF)" ./scripts/chglog.sh init

chglog-test: ## Verify monthly repository changelog scaffolding
	./scripts/test-chglog.sh

proto: ## Generate contracts for SERVICE and tidy its generated module
	$(call require-tool,$(BUF),buf,$(BUF_VERSION))
	@test -f "gen/go/$(SERVICE)/go.mod" || { echo "service $(SERVICE) has no generated-contract module" >&2; exit 2; }
	cd api && "$(BUF)" generate --path "sindri/$(SERVICE)"
	cd "gen/go/$(SERVICE)" && GOWORK=off $(GO) mod tidy

proto-lint: ## Lint and build all Protocol Buffer modules
	$(call require-tool,$(BUF),buf,$(BUF_VERSION))
	cd api && "$(BUF)" lint
	cd api && "$(BUF)" build >/dev/null

proto-breaking-check: ## Reject breaking Proto changes against main
	$(call require-tool,$(BUF),buf,$(BUF_VERSION))
	"$(BUF)" breaking api --against '.git#branch=main,subdir=api'

wire: ## Regenerate Wire output for SERVICE
	$(call require-tool,$(WIRE),wire,$(WIRE_VERSION))
	@test -x "$$(command -v gofmt 2>/dev/null || true)" || { echo 'gofmt is required' >&2; exit 1; }
	@test -d "internal/$(SERVICE)/app" || { echo "unknown service: $(SERVICE)" >&2; exit 2; }
	cd "internal/$(SERVICE)/app" && GOCACHE=$(GOCACHE) WIRE_BIN="$(WIRE)" sh ../../../scripts/generate-wire.sh

go-lint: ## Run golangci-lint once per module
	$(call require-tool,$(GOLANGCI_LINT),golangci-lint,$(GOLANGCI_LINT_VERSION))
	@set -e; for module in $(MODULES); do \
		echo "==> lint $$module"; \
		(cd "$$module" && GOCACHE=$(GOCACHE) GOLANGCI_LINT_CACHE=$(GOLANGCI_LINT_CACHE) "$(GOLANGCI_LINT)" run ./...); \
	done

go-fix: ## Apply Go formatting and safe lint fixes once per module
	$(call require-tool,$(GOLANGCI_LINT),golangci-lint,$(GOLANGCI_LINT_VERSION))
	"$(GOLANGCI_LINT)" fmt
	@set -e; for module in $(MODULES); do \
		(cd "$$module" && GOCACHE=$(GOCACHE) GOLANGCI_LINT_CACHE=$(GOLANGCI_LINT_CACHE) "$(GOLANGCI_LINT)" run --fix ./...); \
	done

fmt-check: ## Verify Go formatting without rewriting source
	$(call require-tool,$(GOLANGCI_LINT),golangci-lint,$(GOLANGCI_LINT_VERSION))
	"$(GOLANGCI_LINT)" fmt --diff

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

service-release-check: ## Validate SERVICE VERSION against published modules
	@test -n "$(VERSION)" || { echo "VERSION is required" >&2; exit 2; }
	GOCACHE=$(GOCACHE) $(GO) run ./tools/service-release-check -service "$(SERVICE)" -version "$(VERSION)"

release-check: proto-lint modules-check ## Validate SERVICE modules before creating module tags

build-sequence-test-image: ## Build the real Sequence process image used by system tests
	@test -x "$$(command -v docker 2>/dev/null || true)" || { echo 'docker is required for sequence system tests' >&2; exit 1; }
	mkdir -p artifacts/sequence
	GOCACHE=$(GOCACHE) CGO_ENABLED=0 GOOS=linux $(GO) build -trimpath -o artifacts/sequence/sequence ./cmd/sequence
	docker build --progress=plain -f tests/sequence/testdata/Dockerfile -t $(SEQUENCE_SYSTEM_IMAGE) .

test-sequence-integration: build-sequence-test-image ## Run Sequence store and E2E tests
	GOCACHE=$(GOCACHE) SKULD_SEQUENCE_TEST_DIALECTS=$(SEQUENCE_TEST_DIALECTS) SKULD_SEQUENCE_TEST_IMAGE=$(SEQUENCE_SYSTEM_IMAGE) $(GO) test -tags=integration -count=1 -timeout=20m ./tests/sequence/...

test-sequence-chaos: build-sequence-test-image ## Run deterministic Sequence system and chaos tests
	GOCACHE=$(GOCACHE) SKULD_SEQUENCE_TEST_DIALECTS=$(SEQUENCE_TEST_DIALECTS) SKULD_SEQUENCE_TEST_IMAGE=$(SEQUENCE_SYSTEM_IMAGE) $(GO) test -tags='integration chaos' -count=1 -timeout=40m ./tests/sequence/...

verify: fmt-check proto-lint scaffold-test release-test chglog-test test test-race build modules-check ## Run the complete merge gate

hooks.install: ## Install pre-commit and commit-msg hooks
	$(call require-tool,$(PRE_COMMIT),pre-commit,$(PRE_COMMIT_VERSION))
	"$(PRE_COMMIT)" install --install-hooks
