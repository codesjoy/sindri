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

mode=${1:-check}
case "$mode" in
	check|write) ;;
	*)
		echo "usage: $0 [check|write]" >&2
		exit 2
		;;
esac

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"

addlicense=${ADDLICENSE_BIN:-bin/addlicense}
copyright_year=${COPYRIGHT_YEAR:-$(date +%Y)}

if [ ! -x "$addlicense" ]; then
	echo "addlicense is missing: $addlicense (run task tools:install)" >&2
	exit 1
fi

# Paths addlicense understands, minus generated and dependency manifests. Files
# that addlicense cannot annotate are discovered by pattern in check mode:
# templates keep the {{YEAR}} placeholder and Dockerfiles carry a header by hand.
scan_paths="cmd internal pkg gen tests tools api deploy releases scripts .github/workflows configs .golangci.yaml .pre-commit-config.yaml cliff.toml Taskfile.yml"

set -- \
	-l apache -c Codesjoy -y "$copyright_year" \
	-ignore '**/*.pb.go' -ignore '**/wire_gen.go' -ignore '**/*.sum' \
	-ignore '**/*.mod' -ignore '**/*.work' -ignore '**/*.lock' \
	-ignore '**/*.tmpl' -ignore 'LICENSE'

if [ "$mode" = write ]; then
	"$addlicense" "$@" $scan_paths
	echo "added Apache 2.0 headers where they were missing"
	exit 0
fi

"$addlicense" -check "$@" $scan_paths

grep -q 'Apache License' LICENSE && grep -q 'Version 2.0' LICENSE || {
	echo "LICENSE is not Apache License 2.0" >&2
	exit 1
}

manual_files=$(
	find scripts/templates/service -type f -name '*.tmpl' ! -name '*.mod.tmpl' 2>/dev/null
	find deploy/docker -maxdepth 1 -type f -name 'Dockerfile*' 2>/dev/null
)
if [ -z "$manual_files" ]; then
	echo "no template or Dockerfile sources found for the license check" >&2
	exit 1
fi
for file in $manual_files; do
	grep -Eq 'Copyright ([{][{]YEAR[}][}]|[0-9]{4}) Codesjoy' "$file" || {
		echo "missing license header: $file" >&2
		exit 1
	}
	grep -q 'Licensed under the Apache License, Version 2.0' "$file" || {
		echo "missing Apache 2.0 header: $file" >&2
		exit 1
	}
done
