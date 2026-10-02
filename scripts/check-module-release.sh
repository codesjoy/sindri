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

service=${1:-}
mode=${2:-check}
case "$mode" in check|prepare) ;; *) echo "invalid module check mode: $mode" >&2; exit 2 ;; esac

if [ -z "$service" ]; then
	for manifest in releases/services/*.yaml; do
		[ -f "$manifest" ] || continue
		name=$(basename "$manifest" .yaml)
		if [ -f "gen/go/$name/go.mod" ] || [ -f "pkg/$name/go.mod" ]; then
			sh scripts/check-module-release.sh "$name" "$mode"
		fi
	done
	exit 0
fi

case "$service" in
	*[!a-z0-9]*|[0-9]*)
		echo "usage: $0 [service] [check|prepare]" >&2
		exit 2
		;;
esac

gen_dir="gen/go/$service"
pkg_dir="pkg/$service"

if [ ! -d "cmd/$service" ] || [ ! -d "internal/$service" ]; then
	echo "missing service directories: cmd/$service and internal/$service" >&2
	exit 1
fi
if [ -f "$pkg_dir/go.mod" ] && [ ! -f "$gen_dir/go.mod" ]; then
	echo "client module requires generated contracts: $gen_dir/go.mod" >&2
	exit 1
fi

modules=
if [ -f "$gen_dir/go.mod" ]; then
	modules="$gen_dir"
fi
if [ -f "$pkg_dir/go.mod" ]; then
	modules="$modules $pkg_dir"
fi
if [ -z "$modules" ]; then
	echo "service $service has no independently publishable modules"
	exit 0
fi

for module_dir in $modules; do
	if grep -Eq '^[[:space:]]*replace([[:space:]]|\()' "$module_dir/go.mod"; then
		echo "$module_dir/go.mod: publishable modules must not contain replace directives" >&2
		exit 1
	fi
	if grep -Eq 'v0\.0\.0([[:space:]]|$)|00010101000000-000000000000' "$module_dir/go.mod"; then
		echo "$module_dir/go.mod: bootstrap versions are not publishable" >&2
		exit 1
	fi
done

tmp_dir=$(mktemp -d)
cleanup() {
	chmod -R u+w "$tmp_dir" 2>/dev/null || true
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM
proxy_dir="$tmp_dir/proxy"
stage_dir="$tmp_dir/stage"
go_bin=${GO_BIN:-"$(go env GOROOT)/bin/go"}
download_proxy="$(go env GOMODCACHE)/cache/download"
mkdir -p "$proxy_dir" "$stage_dir"
metadata=$("$go_bin" run ./tools/service-release-check -service "$service" -mode metadata)
pkg_version=$(printf '%s\n' "$metadata" | awk -v path="$pkg_dir" '$1 == path { print $2 }')
if [ "$mode" = prepare ]; then
	"$go_bin" run ./tools/service-release-check -service "$service" -mode prepare-sums
fi

isolated_go() {
	# GOSUMDB=off keeps the isolated run hermetic: dependency checksums already
	# recorded in go.sum are still enforced, while the local file proxies avoid
	# reaching an external checksum database during candidate preparation.
	env GOWORK=off GOPRIVATE= GONOPROXY=none GOSUMDB=off \
		GONOSUMDB=github.com/codesjoy/sindri GOCACHE="${GOCACHE:-$tmp_dir/gocache}" \
		GOPATH="$tmp_dir/gopath" GOMODCACHE="$tmp_dir/gomodcache" \
		GOPROXY="file://$proxy_dir,file://$download_proxy,https://proxy.golang.org,direct" \
		"$go_bin" "$@"
}

publish_module() {
	module_dir=$1
	version=$2
	module_path=$(sed -n 's/^module[[:space:]]*//p' "$module_dir/go.mod")
	destination="$proxy_dir/$module_path/@v"
	prefix="$module_path@$version"

	mkdir -p "$destination" "$stage_dir/$prefix"
	cp -R "$module_dir"/. "$stage_dir/$prefix/"
	find "$stage_dir/$prefix" -name '.DS_Store' -delete
	cp "$module_dir/go.mod" "$destination/$version.mod"
	printf '{"Version":"%s","Time":"2000-01-01T00:00:00Z"}\n' "$version" >"$destination/$version.info"
	printf '%s\n' "$version" >"$destination/list"
	(cd "$stage_dir" && zip -q -r "$destination/$version.zip" "$prefix")
	rm -rf "$stage_dir/$prefix"
}

test_module() {
	module_dir=$1
	if [ "$mode" = prepare ]; then
		# Go owns checksum generation. Published-version sums remain untouched.
		# Compiling the packages with -mod=mod records the sums Go needs for
		# the candidate dependency while avoiding the full transitive graph
		# (which would require unrelated tool archives). Tests are not run.
		(cd "$module_dir" && isolated_go test -mod=mod -run='^$' ./...)
	fi
	echo "==> GOWORK=off test $module_dir"
	check_dir="$tmp_dir/check/$module_dir"
	mkdir -p "$check_dir"
	cp -R "$module_dir"/. "$check_dir/"
	(
		cd "$check_dir"
		isolated_go test -mod=readonly ./...
	)
}

printf '%s\n' "$metadata" | while read -r module_dir version; do
	test_module "$module_dir"
	publish_module "$module_dir" "$version"
done

if [ "$mode" = prepare ]; then
	# The root module replaces the local candidates with directory paths, so a
	# throwaway modfile with those replacements dropped is used to observe the
	# candidate sums that the isolated service build later verifies read-only.
	# Seeding root go.sum lets Go keep the shared record correctly ordered.
	sums_dir="$tmp_dir/sums"
	mkdir -p "$sums_dir"
	cp go.mod "$sums_dir/go.mod"
	cp go.sum "$sums_dir/go.sum"
	printf '%s\n' "$metadata" | while read -r module_dir version; do
		module_path=$(sed -n 's/^module[[:space:]]*//p' "$module_dir/go.mod")
		"$go_bin" mod edit -modfile="$sums_dir/go.mod" -dropreplace="$module_path"
	done
	printf '%s\n' "$metadata" | while read -r module_dir version; do
		module_path=$(sed -n 's/^module[[:space:]]*//p' "$module_dir/go.mod")
		(
			cd "$sums_dir"
			GOFLAGS="${GOFLAGS:-} -mod=mod" isolated_go mod download "$module_path@$version"
		)
	done
	cp "$sums_dir/go.sum" go.sum
fi

if [ -f "$pkg_dir/go.mod" ]; then
	go_directive=$(sed -n 's/^go[[:space:]]*//p' go.mod | head -n 1)
	if [ -z "$go_directive" ]; then
		echo "go.mod has no go directive to mirror in the consumer fixture" >&2
		exit 1
	fi
	consumer_dir="$tmp_dir/consumer"
	mkdir -p "$consumer_dir"
	cat >"$consumer_dir/go.mod" <<EOF
module example.com/sindri-consumer

go $go_directive

require github.com/codesjoy/sindri/pkg/$service $pkg_version
EOF
	cat >"$consumer_dir/main.go" <<EOF
package main

import _ "github.com/codesjoy/sindri/pkg/$service"

func main() {}
EOF
	echo "==> GOWORK=off build external $pkg_dir consumer"
	(
		cd "$consumer_dir"
		isolated_go build -mod=mod ./...
	)
fi

if [ "${MODULE_CHECK_SERVICE_BUILD:-0}" = 1 ]; then
	cp go.mod "$tmp_dir/service.mod"
	cp go.sum "$tmp_dir/service.sum"
	printf '%s\n' "$metadata" | while read -r module_dir version; do
		module_path=$(sed -n 's/^module[[:space:]]*//p' "$module_dir/go.mod")
		"$go_bin" mod edit -modfile="$tmp_dir/service.mod" -dropreplace="$module_path"
	done
	isolated_go test -mod=readonly -modfile="$tmp_dir/service.mod" "./cmd/$service" "./internal/$service/..."
fi

echo "release checks passed for $service"
