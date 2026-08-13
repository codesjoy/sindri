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
profile=${2:-service}
year=$(date +%Y)
case "$service" in
	''|*[!a-z0-9]*|[0-9]*)
		echo "usage: $0 <lowercase-service-name> [service|contract|client]" >&2
		exit 2
		;;
esac
case "$profile" in
	service|contract|client) ;;
	*)
		echo "invalid profile: $profile (expected service, contract, or client)" >&2
		exit 2
		;;
esac

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"

targets="cmd/$service internal/$service configs/$service.yaml migrations/$service tests/$service .github/workflows/$service.yml"
if [ "$profile" != service ]; then
	targets="api/sindri/$service gen/go/$service $targets"
fi
if [ "$profile" = client ]; then
	targets="pkg/$service $targets"
fi
for target in $targets; do
	if [ -e "$target" ]; then
		echo "refusing to overwrite existing path: $target" >&2
		exit 1
	fi
done

tmp_dir=$(mktemp -d)
work_backup="$tmp_dir/go.work"
cp go.work "$work_backup"
created=
committed=false
cleanup() {
	status=$?
	if [ "$committed" != true ]; then
		for target in $created; do rm -rf "$target"; done
		cp "$work_backup" go.work
	fi
	rm -rf "$tmp_dir"
	exit "$status"
}
trap cleanup EXIT HUP INT TERM

render() {
	sed -e "s/{{SERVICE}}/$service/g" -e "s/{{YEAR}}/$year/g" "$1" >"$2"
}

stage="$tmp_dir/tree"
mkdir -p \
	"$stage/cmd/$service" \
	"$stage/internal/$service/app" \
	"$stage/internal/$service/biz" \
	"$stage/internal/$service/conf" \
	"$stage/internal/$service/data" \
	"$stage/internal/$service/service" \
	"$stage/configs" \
	"$stage/migrations/$service/postgres" \
	"$stage/migrations/$service/mysql" \
	"$stage/tests/$service" \
	"$stage/.github/workflows"

render scripts/templates/service/main.go.tmpl "$stage/cmd/$service/main.go"
render scripts/templates/service/config.yaml.tmpl "$stage/configs/$service.yaml"
render "scripts/templates/service/workflow.$profile.yml.tmpl" "$stage/.github/workflows/$service.yml"
for directory in \
	"$stage/internal/$service/app" \
	"$stage/internal/$service/biz" \
	"$stage/internal/$service/conf" \
	"$stage/internal/$service/data" \
	"$stage/internal/$service/service" \
	"$stage/migrations/$service/postgres" \
	"$stage/migrations/$service/mysql" \
	"$stage/tests/$service"; do
	: >"$directory/.gitkeep"
done

if [ "$profile" != service ]; then
	mkdir -p "$stage/api/sindri/$service/reason" "$stage/api/sindri/$service/v1" "$stage/gen/go/$service"
	render scripts/templates/service/reason.proto.tmpl "$stage/api/sindri/$service/reason/reason.proto"
	render scripts/templates/service/service.proto.tmpl "$stage/api/sindri/$service/v1/$service.proto"
	render scripts/templates/service/gen.go.mod.tmpl "$stage/gen/go/$service/go.mod"
fi
if [ "$profile" = client ]; then
	mkdir -p "$stage/pkg/$service"
	render scripts/templates/service/pkg.go.mod.tmpl "$stage/pkg/$service/go.mod"
	render scripts/templates/service/pkg.doc.go.tmpl "$stage/pkg/$service/doc.go"
fi

for target in $targets; do
	mkdir -p "$(dirname "$target")"
	mv "$stage/$target" "$target"
	created="$target $created"
done

if [ "$profile" != service ]; then
	go work use "./gen/go/$service"
fi
if [ "$profile" = client ]; then
	go work use "./pkg/$service"
fi
committed=true
trap - EXIT HUP INT TERM
rm -rf "$tmp_dir"
echo "created $profile profile for $service"
