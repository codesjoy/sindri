#!/bin/sh
set -eu

go run github.com/google/wire/cmd/wire .

temporary_file="$(mktemp)"
trap 'rm -f "$temporary_file"' EXIT
awk '
  $0 != "//go:generate go run -mod=mod github.com/google/wire/cmd/wire" { print }
' wire_gen.go >"$temporary_file"
mv "$temporary_file" wire_gen.go
trap - EXIT
gofmt -w wire_gen.go
