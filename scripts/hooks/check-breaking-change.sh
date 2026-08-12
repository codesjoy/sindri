#!/usr/bin/env bash

set -euo pipefail

commit_message_file="${1:?commit message file is required}"
commit_subject="$(sed -n '1p' "$commit_message_file")"

if [[ "$commit_subject" == *"!:"* ]]; then
  if ! grep -q '^BREAKING CHANGE:' "$commit_message_file"; then
    echo "commit message policy: bang commits require a BREAKING CHANGE: footer" >&2
    exit 1
  fi
fi
