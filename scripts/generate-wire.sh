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

: "${WIRE_BIN:?WIRE_BIN is required}"
"$WIRE_BIN" .

temporary_file="$(mktemp)"
trap 'rm -f "$temporary_file"' EXIT
awk '
  $0 != "//go:generate go run -mod=mod github.com/google/wire/cmd/wire" { print }
' wire_gen.go >"$temporary_file"
mv "$temporary_file" wire_gen.go
trap - EXIT
gofmt -w wire_gen.go
