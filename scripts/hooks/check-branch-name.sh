#!/usr/bin/env bash

set -euo pipefail

branch_name="$(git symbolic-ref --quiet --short HEAD || true)"

if [[ -z "$branch_name" ]]; then
  echo "branch name policy: detached HEAD is not allowed" >&2
  exit 1
fi

if [[ "$branch_name" =~ ^(main|master|develop)$ || "$branch_name" =~ ^(feature|fix|chore|docs|refactor|release|hotfix)/[a-z0-9][a-z0-9._/-]*$ ]]; then
  exit 0
fi

echo "branch name policy: unsupported branch '$branch_name'" >&2
echo "use main, master, develop, or <type>/<description>" >&2
exit 1
