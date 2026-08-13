#!/usr/bin/env bash
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
