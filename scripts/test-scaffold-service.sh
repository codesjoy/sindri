#!/bin/sh
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
	test -f releases/services/alpha.yaml
	grep -q '^service: alpha$' releases/services/alpha.yaml
	test ! -e releases/services/alpha.md
	test -f migrations/alpha/postgres/.gitkeep
	test -f tests/alpha/.gitkeep
	test -f .github/workflows/alpha.yml
	grep -Eq 'Copyright [0-9]{4} Codesjoy' cmd/alpha/main.go
	grep -Eq 'Copyright [0-9]{4} Codesjoy' configs/alpha.yaml
	grep -Eq 'Copyright [0-9]{4} Codesjoy' .github/workflows/alpha.yml
	test ! -e api/sindri/alpha
	test ! -e gen/go/alpha
	test ! -e pkg/alpha
	test "$(grep -c './cmd/alpha' go.work || true)" -eq 0

	./scripts/scaffold-service.sh bravo contract
	test -f api/sindri/bravo/reason/reason.proto
	test -f gen/go/bravo/go.mod
	test -f cmd/bravo/main.go
	test ! -e pkg/bravo
	grep -Eq 'Copyright [0-9]{4} Codesjoy' api/sindri/bravo/reason/reason.proto
	grep -Eq 'Copyright [0-9]{4} Codesjoy' api/sindri/bravo/v1/bravo.proto
	grep -q './gen/go/bravo' go.work

	./scripts/scaffold-service.sh charlie client
	test -f api/sindri/charlie/reason/reason.proto
	test -f gen/go/charlie/go.mod
	test -f pkg/charlie/go.mod
	test -f internal/charlie/service/.gitkeep
	grep -Eq 'Copyright [0-9]{4} Codesjoy' pkg/charlie/doc.go
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
