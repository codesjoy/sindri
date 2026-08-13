#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
fixture="$tmp_dir/repo"
go_bin="$(go env GOROOT)/bin/go"

mkdir -p "$fixture/scripts" "$fixture/cmd/solo" "$fixture/internal/solo" \
	"$fixture/gen/go/contract" "$fixture/cmd/contract" "$fixture/internal/contract" \
	"$fixture/gen/go/client" "$fixture/pkg/client" "$fixture/cmd/client" "$fixture/internal/client"
cp "$repo_root/scripts/check-module-release.sh" "$fixture/scripts/"

cat >"$fixture/gen/go/contract/go.mod" <<'EOF'
module github.com/codesjoy/sindri/gen/go/contract

go 1.26.4
EOF
cat >"$fixture/gen/go/contract/contract.go" <<'EOF'
package contract
EOF
cat >"$fixture/gen/go/client/go.mod" <<'EOF'
module github.com/codesjoy/sindri/gen/go/client

go 1.26.4
EOF
cat >"$fixture/gen/go/client/client.go" <<'EOF'
package client
EOF
cat >"$fixture/pkg/client/go.mod" <<'EOF'
module github.com/codesjoy/sindri/pkg/client

go 1.26.4

require github.com/codesjoy/sindri/gen/go/client v0.1.0
EOF
cat >"$fixture/pkg/client/client.go" <<'EOF'
package client
EOF

(
	cd "$fixture"
	GO_BIN="$go_bin" ./scripts/check-module-release.sh solo
	GO_BIN="$go_bin" ./scripts/check-module-release.sh contract
	GO_BIN="$go_bin" ./scripts/check-module-release.sh client

	sed 's/v0.1.0/v0.0.0/' pkg/client/go.mod >pkg/client/go.mod.invalid
	mv pkg/client/go.mod.invalid pkg/client/go.mod
	if GO_BIN="$go_bin" ./scripts/check-module-release.sh client >/dev/null 2>&1; then
		echo "bootstrap version unexpectedly passed release validation" >&2
		exit 1
	fi

	mkdir -p cmd/broken internal/broken pkg/broken
	cp pkg/client/client.go pkg/broken/client.go
	cp pkg/client/go.mod pkg/broken/go.mod
	if GO_BIN="$go_bin" ./scripts/check-module-release.sh broken >/dev/null 2>&1; then
		echo "client without generated contracts unexpectedly passed" >&2
		exit 1
	fi
)

echo "optional module release checks passed"
