#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
fixture="$tmp_dir/repo"

mkdir -p "$fixture/scripts/templates/service" "$fixture/.github/workflows"
cp "$repo_root/scripts/scaffold-service.sh" "$fixture/scripts/"
cp "$repo_root/scripts/templates/service"/* "$fixture/scripts/templates/service/"
printf 'go 1.26.4\n' >"$fixture/go.work"

(
	cd "$fixture"

	./scripts/scaffold-service.sh alpha
	test -f cmd/alpha/main.go
	test -f internal/alpha/app/.gitkeep
	test -f configs/alpha.yaml
	test -f migrations/alpha/postgres/.gitkeep
	test -f tests/alpha/.gitkeep
	test -f .github/workflows/alpha.yml
	test ! -e api/sindri/alpha
	test ! -e gen/go/alpha
	test ! -e pkg/alpha
	test "$(grep -c './cmd/alpha' go.work || true)" -eq 0

	./scripts/scaffold-service.sh bravo contract
	test -f api/sindri/bravo/reason/reason.proto
	test -f gen/go/bravo/go.mod
	test -f cmd/bravo/main.go
	test ! -e pkg/bravo
	grep -q './gen/go/bravo' go.work

	./scripts/scaffold-service.sh charlie client
	test -f api/sindri/charlie/reason/reason.proto
	test -f gen/go/charlie/go.mod
	test -f pkg/charlie/go.mod
	test -f internal/charlie/service/.gitkeep
	grep -q './pkg/charlie' go.work

	if ./scripts/scaffold-service.sh Alpha >/dev/null 2>&1; then
		echo "invalid service name unexpectedly succeeded" >&2
		exit 1
	fi
	if ./scripts/scaffold-service.sh delta unknown >/dev/null 2>&1; then
		echo "invalid profile unexpectedly succeeded" >&2
		exit 1
	fi
	if ./scripts/scaffold-service.sh alpha >/dev/null 2>&1; then
		echo "existing service unexpectedly succeeded" >&2
		exit 1
	fi

	cp go.work "$tmp_dir/work.before"
	mkdir -p pkg/collision
	if ./scripts/scaffold-service.sh collision client >/dev/null 2>&1; then
		echo "colliding client profile unexpectedly succeeded" >&2
		exit 1
	fi
	cmp go.work "$tmp_dir/work.before"
	test ! -e api/sindri/collision
	test ! -e gen/go/collision
	test ! -e cmd/collision
	test ! -e internal/collision
)

echo "scaffold service checks passed"
