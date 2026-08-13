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

TOOL_DIR=${TOOL_DIR:?}
TOOL_STAMP_DIR=${TOOL_STAMP_DIR:?}
GO_BIN=${GO_BIN:-go}
PYTHON_BIN=${PYTHON_BIN:-python3}
ADDLICENSE_VERSION=${ADDLICENSE_VERSION:?}
BUF_VERSION=${BUF_VERSION:?}
WIRE_VERSION=${WIRE_VERSION:?}
GOLANGCI_LINT_VERSION=${GOLANGCI_LINT_VERSION:?}
PRE_COMMIT_VERSION=${PRE_COMMIT_VERSION:?}
GIT_CLIFF_VERSION=${GIT_CLIFF_VERSION:?}

mkdir -p "$TOOL_DIR" "$TOOL_STAMP_DIR"

install_go_tool() {
	name=$1
	version=$2
	package=$3
	stamp="$TOOL_STAMP_DIR/$name-$version"
	binary="$TOOL_DIR/$name"
	if [ ! -x "$binary" ] || [ ! -f "$stamp" ]; then
		echo "==> install $name $version"
		GOBIN="$TOOL_DIR" "$GO_BIN" install "$package"
		find "$TOOL_STAMP_DIR" -maxdepth 1 -type f -name "$name-*" -delete
		: >"$stamp"
	fi
}

sha256_check() {
	file=$1
	expected=$2
	if command -v sha256sum >/dev/null 2>&1; then
		actual=$(sha256sum "$file" | awk '{print $1}')
	else
		actual=$(shasum -a 256 "$file" | awk '{print $1}')
	fi
	[ "$actual" = "$expected" ] || { echo "checksum mismatch for $file" >&2; exit 1; }
}

install_git_cliff() {
	name=git-cliff
	version=$GIT_CLIFF_VERSION
	stamp="$TOOL_STAMP_DIR/$name-$version"
	binary="$TOOL_DIR/$name"
	tmp="$TOOL_DIR/.install-$name"
	if [ -x "$binary" ] && [ -f "$stamp" ]; then
		return
	fi

	os=$(uname -s)
	arch=$(uname -m)
	case "$os/$arch" in
		Darwin/arm64)
			target=aarch64-apple-darwin
			checksum=21547ae4a0421164070ab75c2522864ea5565858a011fabc5f583061b20f1226
			;;
		Darwin/x86_64)
			target=x86_64-apple-darwin
			checksum=6e60ae390d375cecb9d8008c49f0e724a8dfe40390b532ef5501e421d2cc8acb
			;;
		Linux/aarch64|Linux/arm64)
			target=aarch64-unknown-linux-musl
			checksum=4054c124b926c117f3fa048939bc8be0a954f29f3b6f367627e8cb22c1971882
			;;
		Linux/x86_64)
			target=x86_64-unknown-linux-musl
			checksum=200d2535da6d9703f3bcc8a4d159c3b55eacdb01cf2148c55b3eee9dd04d5249
			;;
		*)
			echo "unsupported git-cliff platform: $os/$arch" >&2
			exit 1
			;;
	esac

	archive="git-cliff-$version-$target.tar.gz"
	url="https://github.com/orhun/git-cliff/releases/download/v$version/$archive"
	echo "==> install $name $version ($target)"
	rm -rf "$tmp"
	mkdir -p "$tmp"
	curl -fsSL "$url" -o "$tmp/$archive"
	sha256_check "$tmp/$archive" "$checksum"
	tar -xzf "$tmp/$archive" -C "$tmp"
	cp "$tmp/git-cliff-$version/$name" "$binary"
	chmod +x "$binary"
	rm -rf "$tmp"
	find "$TOOL_STAMP_DIR" -maxdepth 1 -type f -name "$name-*" -delete
	: >"$stamp"
}

install_pre_commit() {
	name=pre-commit
	stamp="$TOOL_STAMP_DIR/$name-$PRE_COMMIT_VERSION"
	binary="$TOOL_DIR/pre-commit-venv/bin/pre-commit"
	if [ -x "$binary" ] && [ -f "$stamp" ]; then
		return
	fi

	echo "==> install $name $PRE_COMMIT_VERSION"
	"$PYTHON_BIN" -m venv "$TOOL_DIR/pre-commit-venv"
	"$TOOL_DIR/pre-commit-venv/bin/pip" install --disable-pip-version-check "pre-commit==$PRE_COMMIT_VERSION"
	find "$TOOL_STAMP_DIR" -maxdepth 1 -type f -name "$name-*" -delete
	: >"$stamp"
}

install_go_tool addlicense "$ADDLICENSE_VERSION" "github.com/google/addlicense@$ADDLICENSE_VERSION"
install_go_tool buf "$BUF_VERSION" "github.com/bufbuild/buf/cmd/buf@$BUF_VERSION"
install_go_tool wire "$WIRE_VERSION" "github.com/google/wire/cmd/wire@$WIRE_VERSION"
install_go_tool golangci-lint "$GOLANGCI_LINT_VERSION" "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$GOLANGCI_LINT_VERSION"
install_git_cliff
install_pre_commit
