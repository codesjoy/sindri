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
ADDLICENSE_VERSION=${ADDLICENSE_VERSION:?}
BUF_VERSION=${BUF_VERSION:?}
WIRE_VERSION=${WIRE_VERSION:?}
GOLANGCI_LINT_VERSION=${GOLANGCI_LINT_VERSION:?}
PREK_VERSION=${PREK_VERSION:?}
GIT_CLIFF_VERSION=${GIT_CLIFF_VERSION:?}

mode=${1:-install}
case "$mode" in
	install|check) ;;
	*)
		echo "usage: $0 [install|check]" >&2
		exit 2
		;;
esac

# check_tools mirrors the install calls at the end of this file so the tool
# names and version stamps have a single owner.
check_tools() {
	missing=0
	for entry in \
		"addlicense|$TOOL_DIR/addlicense|$ADDLICENSE_VERSION" \
		"buf|$TOOL_DIR/buf|$BUF_VERSION" \
		"wire|$TOOL_DIR/wire|$WIRE_VERSION" \
		"golangci-lint|$TOOL_DIR/golangci-lint|$GOLANGCI_LINT_VERSION" \
		"git-cliff|$TOOL_DIR/git-cliff|$GIT_CLIFF_VERSION" \
		"prek|$TOOL_DIR/prek|$PREK_VERSION"; do
		name=${entry%%|*}
		rest=${entry#*|}
		binary=${rest%%|*}
		version=${rest#*|}
		if [ ! -x "$binary" ] || [ ! -f "$TOOL_STAMP_DIR/$name-$version" ]; then
			echo "missing tool or version stamp: $name $version (run task tools:install)" >&2
			missing=1
		fi
	done
	[ "$missing" -eq 0 ]
}

if [ "$mode" = check ]; then
	check_tools
	exit 0
fi

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

install_github_tarball_tool() {
	name=$1
	version=$2
	target=$3
	checksum=$4
	url=$5
	extracted_binary=$6
	stamp="$TOOL_STAMP_DIR/$name-$version"
	binary="$TOOL_DIR/$name"
	tmp="$TOOL_DIR/.install-$name"
	if [ -x "$binary" ] && [ -f "$stamp" ]; then
		return
	fi

	archive=${url##*/}
	echo "==> install $name $version ($target)"
	rm -rf "$tmp"
	mkdir -p "$tmp"
	curl -fsSL "$url" -o "$tmp/$archive"
	sha256_check "$tmp/$archive" "$checksum"
	tar -xzf "$tmp/$archive" -C "$tmp"
	cp "$tmp/$extracted_binary" "$binary"
	chmod +x "$binary"
	rm -rf "$tmp"
	find "$TOOL_STAMP_DIR" -maxdepth 1 -type f -name "$name-*" -delete
	: >"$stamp"
}

install_git_cliff() {
	version=$GIT_CLIFF_VERSION
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

	install_github_tarball_tool git-cliff "$version" "$target" "$checksum" \
		"https://github.com/orhun/git-cliff/releases/download/v$version/git-cliff-$version-$target.tar.gz" \
		"git-cliff-$version/git-cliff"
}

install_prek() {
	version=$PREK_VERSION
	os=$(uname -s)
	arch=$(uname -m)
	case "$os/$arch" in
		Darwin/arm64)
			target=aarch64-apple-darwin
			checksum=88eec06dd10fd61a9b345223fafc9bedb6746ee4cc47377551239d89d752b365
			;;
		Darwin/x86_64)
			target=x86_64-apple-darwin
			checksum=bc3ddd3a5686fa97120ec2d09db9e7353df4311a85e7b00944342ddd9c4a2035
			;;
		Linux/aarch64|Linux/arm64)
			target=aarch64-unknown-linux-musl
			checksum=5ba9d0dc3d4add8cc2c569f4fa27b56d6a042a59f6ee008ec9d7790cece5a200
			;;
		Linux/x86_64)
			target=x86_64-unknown-linux-musl
			checksum=805632185f4539ca2eb0fd1b3b52ea842cecbc4ba1e749be430df948621353b0
			;;
		*)
			echo "unsupported prek platform: $os/$arch" >&2
			exit 1
			;;
	esac

	install_github_tarball_tool prek "$version" "$target" "$checksum" \
		"https://github.com/j178/prek/releases/download/v$version/prek-$target.tar.gz" \
		"prek-$target/prek"

	# prek replaced the Python-venv pre-commit runner; drop its leftovers so
	# bin/ and its version stamps keep a single owner.
	rm -rf "$TOOL_DIR/pre-commit-venv"
	find "$TOOL_STAMP_DIR" -maxdepth 1 -type f -name 'pre-commit-*' -delete
}

install_go_tool addlicense "$ADDLICENSE_VERSION" "github.com/google/addlicense@$ADDLICENSE_VERSION"
install_go_tool buf "$BUF_VERSION" "github.com/bufbuild/buf/cmd/buf@$BUF_VERSION"
install_go_tool wire "$WIRE_VERSION" "github.com/google/wire/cmd/wire@$WIRE_VERSION"
install_go_tool golangci-lint "$GOLANGCI_LINT_VERSION" "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$GOLANGCI_LINT_VERSION"
install_git_cliff
install_prek
