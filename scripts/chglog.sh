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

mode=${1:-}
month=${2:-}
git_cliff=${GIT_CLIFF_BIN:-bin/git-cliff}

usage() {
	echo "usage: $0 month [YYYY-MM] | init" >&2
	exit 2
}

valid_month() {
	case "$1" in
		[0-9][0-9][0-9][0-9]-[0-9][0-9]) ;;
		*) return 1 ;;
	esac
	year=${1%-*}
	month_number=${1#*-}
	[ "$year" -ge 1970 ] 2>/dev/null || return 1
	[ "$month_number" -ge 1 ] 2>/dev/null || return 1
	[ "$month_number" -le 12 ] 2>/dev/null || return 1
}

case "$mode" in
	month)
		if [ -z "$month" ]; then
			month=$(date +%Y-%m)
		fi
		valid_month "$month" || usage
		;;
	init)
		[ -z "$month" ] || usage
		;;
	*) usage ;;
esac

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$repo_root"

changelog="CHANGELOG.md"

if [ ! -x "$git_cliff" ]; then
	echo "git-cliff is missing: $git_cliff (run make tools.install)" >&2
	exit 1
fi
if [ ! -f cliff.toml ]; then
	echo "missing git-cliff configuration: cliff.toml" >&2
	exit 1
fi

next_month() {
	year=${1%-*}
	month_number=${1#*-}
	if [ "$month_number" -eq 12 ]; then
		printf '%04d-01\n' "$((year + 1))"
	else
		printf '%04d-%02d\n' "$year" "$((10#$month_number + 1))"
	fi
}

month_range() {
	start="$1-01 00:00:00"
	end="$(next_month "$1")-01 00:00:00"
	first=$(git rev-list --reverse --since="$start" --until="$end" HEAD | sed -n '1p')
	if [ -z "$first" ]; then
		return 1
	fi
	last=$(git rev-list --since="$start" --until="$end" HEAD | sed -n '1p')
	if git rev-parse "$first^" >/dev/null 2>&1; then
		printf '%s^..%s\n' "$first" "$last"
	else
		printf '%s\n' "$last"
	fi
}

render_month() {
	render_month_value=$1
	render_output_path=$2
	if ! range=$(month_range "$render_month_value"); then
		return 1
	fi
	"$git_cliff" --config cliff.toml --ignore-tags '.*' --tag "$render_month_value" --strip all "$range" >"$render_output_path"
}

replace_month() {
	month=$1
	section=$2
	target=$3
	tmp=$4
	if [ ! -f "$target" ]; then
		printf '# Changelog\n\n' >"$target"
	fi
	awk -v month="$month" -v section="$section" '
		BEGIN {
			inserted = 0
			skipping = 0
		}
		NR == 1 {
			print
			next
		}
		/^## / {
			if ($0 == "## " month) {
				if (!inserted) {
					while ((getline line < section) > 0) {
						print line
					}
					close(section)
					inserted = 1
				}
				skipping = 1
				next
			}
			if (skipping) {
				skipping = 0
			}
		}
		!skipping {
			print
		}
		END {
			if (!inserted) {
				print ""
				while ((getline line < section) > 0) {
					print line
				}
				close(section)
			}
		}
	' "$target" >"$tmp"
	mv "$tmp" "$target"
}

write_month() {
	month=$1
	tmp_dir=$2
	section="$tmp_dir/$month.md"
	next="$tmp_dir/CHANGELOG.md"
	if ! render_month "$month" "$section"; then
		echo "no commits found for $month" >&2
		return 0
	fi
	if ! grep -Eq '^- ' "$section"; then
		echo "no public changelog entries for $month" >&2
		return 0
	fi
	replace_month "$month" "$section" "$changelog" "$next"
}

case "$mode" in
	month)
		tmp_dir=$(mktemp -d)
		trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
		write_month "$month" "$tmp_dir"
		;;
	init)
		tmp_dir=$(mktemp -d)
		trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
		months="$tmp_dir/months"
		output="$tmp_dir/CHANGELOG.md"
		git log --format=%cs HEAD | cut -c1-7 | sort -ur >"$months"
		printf '# Changelog\n' >"$output"
		while IFS= read -r month; do
			[ -n "$month" ] || continue
			section="$tmp_dir/$month.md"
			if ! render_month "$month" "$section"; then
				continue
			fi
			if ! grep -Eq '^- ' "$section"; then
				continue
			fi
			printf '\n' >>"$output"
			cat "$section" >>"$output"
		done <"$months"
		if ! grep -Eq '^- ' "$output"; then
			echo "no public changelog entries found" >&2
			exit 1
		fi
		mv "$output" "$changelog"
		;;
esac

echo "updated $changelog"
