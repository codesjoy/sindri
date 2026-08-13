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

mkdir -p "$fixture/scripts" "$fixture/bin"
cp "$repo_root/scripts/chglog.sh" "$fixture/scripts/"
cp "$repo_root/cliff.toml" "$fixture/"
cat >"$fixture/bin/git-cliff" <<'SH'
#!/bin/sh
set -eu
: "${GIT_CLIFF_ARGS_DIR:?}"
mkdir -p "$GIT_CLIFF_ARGS_DIR"
call_file="$GIT_CLIFF_ARGS_DIR/call-$PPID-$$.args"
printf '%s\n' "$@" >"$call_file"
month=
while [ "$#" -gt 0 ]; do
	case "$1" in
		--tag)
			month=$2
			shift 2
			;;
		*)
			shift
			;;
	esac
done
case "$month" in
	2026-08)
		cat <<'OUT'
## 2026-08

### Features

- **sequence:** Add sequence service

### Bug fixes

- **sequence:** Harden route convergence and recovery
OUT
		;;
	2026-07)
		cat <<'OUT'
## 2026-07

### Refactor

- **sequence:** Separate publishable modules **BREAKING**
OUT
		;;
	*)
		printf '## %s\n\n' "$month"
		;;
esac
SH
chmod +x "$fixture/bin/git-cliff"

(
	cd "$fixture"
	git init -q
	git config user.name "Changelog Test"
	git config user.email "chglog@example.test"
	printf 'root\n' >README.md
	git add README.md
	GIT_AUTHOR_DATE='2026-07-10T12:00:00+0000' GIT_COMMITTER_DATE='2026-07-10T12:00:00+0000' \
		git commit -q -m 'refactor(sequence)!: separate publishable modules'
	printf 'feature\n' >>README.md
	GIT_AUTHOR_DATE='2026-08-13T12:00:00+0000' GIT_COMMITTER_DATE='2026-08-13T12:00:00+0000' \
		git commit -q -am 'feat(sequence): add sequence service'

	GIT_CLIFF_BIN="$PWD/bin/git-cliff" GIT_CLIFF_ARGS_DIR="$tmp_dir/args" ./scripts/chglog.sh month 2026-08
	test -f CHANGELOG.md
	test "$(grep -c '^## 2026-08$' CHANGELOG.md)" -eq 1
	grep -q 'Add sequence service' CHANGELOG.md
	grep -R -Fxq -- '--config' "$tmp_dir/args"
	grep -R -Fxq -- 'cliff.toml' "$tmp_dir/args"
	grep -R -Fxq -- '--ignore-tags' "$tmp_dir/args"
	grep -R -Fxq -- '.*' "$tmp_dir/args"
	grep -R -Fxq -- '--tag' "$tmp_dir/args"
	grep -R -Fxq -- '2026-08' "$tmp_dir/args"
	grep -R -Fxq -- '--strip' "$tmp_dir/args"
	grep -R -Fxq -- 'all' "$tmp_dir/args"
	if grep -R -Fxq -- '--tag-pattern' "$tmp_dir/args"; then
		echo "monthly changelog unexpectedly used tag pattern filtering" >&2
		exit 1
	fi

	printf '# Changelog\n\n## 2026-08\n\nold content\n\n## 2026-07\n\nold july\n' >CHANGELOG.md
	GIT_CLIFF_BIN="$PWD/bin/git-cliff" GIT_CLIFF_ARGS_DIR="$tmp_dir/replace-args" ./scripts/chglog.sh month 2026-08
	test "$(grep -c '^## 2026-08$' CHANGELOG.md)" -eq 1
	test "$(grep -c '^## 2026-07$' CHANGELOG.md)" -eq 1
	! grep -q 'old content' CHANGELOG.md
	grep -q 'old july' CHANGELOG.md

	rm -f CHANGELOG.md
	GIT_CLIFF_BIN="$PWD/bin/git-cliff" GIT_CLIFF_ARGS_DIR="$tmp_dir/init-args" ./scripts/chglog.sh init
	test -f CHANGELOG.md
	grep -q '^## 2026-08$' CHANGELOG.md
	grep -q '^## 2026-07$' CHANGELOG.md
	grep -q 'Separate publishable modules' CHANGELOG.md

	if GIT_CLIFF_BIN="$PWD/bin/git-cliff" GIT_CLIFF_ARGS_DIR="$tmp_dir/invalid-short" ./scripts/chglog.sh month 2026-8 >/dev/null 2>&1; then
		echo "short month unexpectedly succeeded" >&2
		exit 1
	fi
	if GIT_CLIFF_BIN="$PWD/bin/git-cliff" GIT_CLIFF_ARGS_DIR="$tmp_dir/invalid-name" ./scripts/chglog.sh month bad >/dev/null 2>&1; then
		echo "invalid month unexpectedly succeeded" >&2
		exit 1
	fi
	if GIT_CLIFF_BIN="$PWD/bin/git-cliff" GIT_CLIFF_ARGS_DIR="$tmp_dir/invalid-range" ./scripts/chglog.sh month 2026-13 >/dev/null 2>&1; then
		echo "out-of-range month unexpectedly succeeded" >&2
		exit 1
	fi
)

grep -q 'message = "^feat"' "$repo_root/cliff.toml"
grep -q 'message = "^fix"' "$repo_root/cliff.toml"
grep -q 'message = "^perf"' "$repo_root/cliff.toml"
grep -q 'message = "^refactor"' "$repo_root/cliff.toml"
grep -q 'skip = true' "$repo_root/cliff.toml"
if grep -Eq 'message = "\^(test|build|ci|chore|docs?)"' "$repo_root/cliff.toml"; then
	echo "maintenance commit types should not be changelog groups" >&2
	exit 1
fi

echo "monthly repository changelog checks passed"
