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
version=${2:-v0.1.0}

case "$service" in
	''|*[!a-z0-9]*|[0-9]*)
		echo "usage: $0 <service> [vMAJOR.MINOR.PATCH]" >&2
		exit 2
		;;
esac
case "$version" in
	v[0-9]*.[0-9]*.[0-9]*) ;;
	*) echo "invalid semantic version: $version" >&2; exit 2 ;;
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

publish_module() {
	module_dir=$1
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
	echo "==> GOWORK=off test $module_dir"
	check_dir="$tmp_dir/check/$module_dir"
	mkdir -p "$check_dir"
	cp -R "$module_dir"/. "$check_dir/"
	(
		cd "$check_dir"
		env GOWORK=off GOPRIVATE= GONOPROXY=none \
			GONOSUMDB=github.com/codesjoy/sindri GOCACHE="$tmp_dir/gocache" \
			GOPATH="$tmp_dir/gopath" GOMODCACHE="$tmp_dir/gomodcache" \
			GOPROXY="file://$proxy_dir,file://$download_proxy,https://proxy.golang.org,direct" \
			"$go_bin" test -mod=mod ./...
	)
}

for module_dir in $modules; do
	test_module "$module_dir"
	publish_module "$module_dir"
done

if [ -f "$pkg_dir/go.mod" ]; then
	consumer_dir="$tmp_dir/consumer"
	mkdir -p "$consumer_dir"
	cat >"$consumer_dir/go.mod" <<EOF
module example.com/sindri-consumer

go 1.26.4

require github.com/codesjoy/sindri/pkg/$service $version
EOF
	cat >"$consumer_dir/main.go" <<EOF
package main

import _ "github.com/codesjoy/sindri/pkg/$service"

func main() {}
EOF
	echo "==> GOWORK=off build external $pkg_dir consumer"
	(
		cd "$consumer_dir"
		env GOWORK=off GOPRIVATE= GONOPROXY=none \
			GONOSUMDB=github.com/codesjoy/sindri GOCACHE="$tmp_dir/gocache" \
			GOPATH="$tmp_dir/gopath" GOMODCACHE="$tmp_dir/gomodcache" \
			GOPROXY="file://$proxy_dir,file://$download_proxy,https://proxy.golang.org,direct" \
			"$go_bin" build -mod=mod ./...
	)
fi

echo "release checks passed for $service"
