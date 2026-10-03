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
chmod +x "$fixture/scripts/chglog.sh"

cat >"$fixture/bin/git-cliff" <<'SH'
#!/bin/sh
set -eu
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
	2026-07)
		printf '## 2026-07\n\n\n### Features\n\n- **sequence:** Add july feature\n\n'
		;;
	2026-08)
		printf '## 2026-08\n\n\n### Features\n\n- **sequence:** Add august feature\n\n'
		;;
	2026-09)
		printf '## 2026-09\n\n\n### Features\n\n- **sequence:** Add september feature\n\n'
		;;
	2026-10)
		printf '## 2026-10\n\n\n### Features\n\n- **sequence:** Add october feature\n\n'
		;;
	*)
		printf '## %s\n' "$month"
		;;
esac
SH
chmod +x "$fixture/bin/git-cliff"

(
	cd "$fixture"
	git init -q
	git config user.name "Changelog Test"
	git config user.email "chglog@example.test"

	commit_month() {
		commit_month_value=$1
		commit_month_message=$2
		printf '%s\n' "$commit_month_value" >>README.md
		git add README.md
		GIT_AUTHOR_DATE="$commit_month_value-12T12:00:00+0000" \
			GIT_COMMITTER_DATE="$commit_month_value-12T12:00:00+0000" \
			git commit -q -m "$commit_month_message"
	}

	commit_month 2026-07 'feat(sequence): add july feature'
	commit_month 2026-08 'feat(sequence): add august feature'
	commit_month 2026-09 'feat(sequence): add september feature'
	commit_month 2026-10 'feat(sequence): add october feature'

	cliff_bin="$PWD/bin/git-cliff"

	run_month() {
		GIT_CLIFF_BIN="$cliff_bin" ./scripts/chglog.sh month "$1"
	}

	all_months() {
		grep -E '^## [0-9]{4}-[0-9]{2}$' CHANGELOG.md
	}

	# require_single_blank_before fails unless exactly one blank line separates
	# the given month heading from the content above it.
	require_single_blank_before() {
		awk -v heading="## $1" '
			{ lines[NR] = $0 }
			END {
				found = 0
				for (i = 3; i <= NR; i++) {
					if (lines[i] == heading) {
						found = 1
						if (lines[i - 1] != "" || lines[i - 2] == "") {
							exit 1
						}
					}
				}
				if (!found) {
					exit 1
				}
			}
		' CHANGELOG.md
	}

	run_month 2026-08
	test -f CHANGELOG.md
	test "$(all_months)" = '## 2026-08'
	grep -q 'Add august feature' CHANGELOG.md
	require_single_blank_before 2026-08
	test -n "$(tail -n 1 CHANGELOG.md)"

	# A newer month must be inserted at the top instead of appended.
	run_month 2026-10
	test "$(all_months)" = "$(printf '## 2026-10\n## 2026-08')"
	require_single_blank_before 2026-10
	require_single_blank_before 2026-08
	test -n "$(tail -n 1 CHANGELOG.md)"

	# Backfilling an older month lands between the newer and older sections.
	run_month 2026-09
	test "$(all_months)" = "$(printf '## 2026-10\n## 2026-09\n## 2026-08')"
	require_single_blank_before 2026-09
	require_single_blank_before 2026-08
	test -n "$(tail -n 1 CHANGELOG.md)"

	# Re-running a month replaces its section without duplicating it.
	run_month 2026-09
	test "$(all_months)" = "$(printf '## 2026-10\n## 2026-09\n## 2026-08')"
	test "$(grep -c '^## 2026-09$' CHANGELOG.md)" -eq 1

	# A legacy reversed file is rewritten into descending month order.
	printf '# Changelog\n\n## 2026-08\n\nold august\n\n## 2026-10\n\nold october\n' >CHANGELOG.md
	run_month 2026-10
	test "$(all_months)" = "$(printf '## 2026-10\n## 2026-08')"
	test "$(grep -c '^## 2026-10$' CHANGELOG.md)" -eq 1
	! grep -q 'old october' CHANGELOG.md
	grep -q 'old august' CHANGELOG.md
	grep -q 'Add october feature' CHANGELOG.md

	# init renders every commit month newest first.
	rm -f CHANGELOG.md
	GIT_CLIFF_BIN="$cliff_bin" ./scripts/chglog.sh init
	test "$(all_months)" = "$(printf '## 2026-10\n## 2026-09\n## 2026-08\n## 2026-07')"
	grep -q 'Add september feature' CHANGELOG.md
	grep -q 'Add july feature' CHANGELOG.md
	require_single_blank_before 2026-10
	require_single_blank_before 2026-09
	require_single_blank_before 2026-08
	require_single_blank_before 2026-07
	test -n "$(tail -n 1 CHANGELOG.md)"

	for invalid in 2026-8 bad 2026-13; do
		if GIT_CLIFF_BIN="$cliff_bin" ./scripts/chglog.sh month "$invalid" >/dev/null 2>&1; then
			echo "month $invalid unexpectedly succeeded" >&2
			exit 1
		fi
	done
)

grep -q 'sort_commits = "newest"' "$repo_root/cliff.toml"
grep -q 'commit.breaking_description' "$repo_root/cliff.toml"
grep -q '### Breaking changes' "$repo_root/cliff.toml"

echo "monthly repository changelog checks passed"
