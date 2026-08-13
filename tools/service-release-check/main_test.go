// Copyright 2026 Codesjoy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckerProfiles(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
	}{
		{
			name: "service only",
			manifest: `service: alpha
releases:
  - version: v0.1.0
`,
		},
		{
			name: "contract only",
			manifest: `service: alpha
releases:
  - version: v0.1.0
    contract: gen/go/alpha/v0.1.0
`,
		},
		{
			name: "contract and client",
			manifest: `service: alpha
releases:
  - version: v0.1.0
    contract: gen/go/alpha/v0.1.0
    tested_clients:
      - pkg/alpha/v0.1.0
`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := newFixture(t, "v0.1.0")
			writeFile(t, repo, "releases/services/alpha.yaml", test.manifest)
			if err := (&checker{repo: repo, skipBuild: true}).run("alpha", "v0.1.0"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCheckerRejectsInvalidIdentity(t *testing.T) {
	check := &checker{repo: t.TempDir(), skipBuild: true}
	for _, test := range []struct {
		service string
		version string
	}{
		{service: "Bad", version: "v0.1.0"},
		{service: "alpha", version: "1.0.0"},
	} {
		if err := check.run(test.service, test.version); err == nil {
			t.Fatalf("run(%q, %q) unexpectedly passed", test.service, test.version)
		}
	}
}

func TestCheckerRejectsInvalidRelease(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
		prepare  func(*testing.T, string)
		contains string
	}{
		{
			name: "missing tag",
			manifest: `service: alpha
releases:
  - version: v0.1.0
    contract: gen/go/alpha/v9.9.9
`,
			contains: "missing module tag",
		},
		{
			name: "client without contract",
			manifest: `service: alpha
releases:
  - version: v0.1.0
    tested_clients:
      - pkg/alpha/v0.1.0
`,
			contains: "tested clients require a contract",
		},
		{
			name: "dependency mismatch",
			manifest: `service: alpha
releases:
  - version: v0.1.0
    contract: gen/go/alpha/v0.1.0
    tested_clients:
      - pkg/alpha/v0.1.0
`,
			prepare: func(t *testing.T, repo string) {
				git(t, repo, "tag", "-d", "pkg/alpha/v0.1.0")
				writeFile(t, repo, "pkg/alpha/go.mod", `module github.com/codesjoy/sindri/pkg/alpha

go 1.26.4

require github.com/codesjoy/sindri/gen/go/alpha v0.2.0
`)
				git(t, repo, "add", ".")
				git(t, repo, "commit", "-m", "change client dependency")
				git(t, repo, "tag", "pkg/alpha/v0.1.0")
			},
			contains: "want v0.1.0",
		},
		{
			name: "source drift",
			manifest: `service: alpha
releases:
  - version: v0.1.0
    contract: gen/go/alpha/v0.1.0
    tested_clients:
      - pkg/alpha/v0.1.0
`,
			prepare: func(t *testing.T, repo string) {
				writeFile(t, repo, "pkg/alpha/client.go", "package alpha\n\nconst changed = true\n")
			},
			contains: "module tree differs",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := newFixture(t, "v0.1.0")
			writeFile(t, repo, "releases/services/alpha.yaml", test.manifest)
			if test.prepare != nil {
				test.prepare(t, repo)
			}
			err := (&checker{repo: repo, skipBuild: true}).run("alpha", "v0.1.0")
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("error = %v, want substring %q", err, test.contains)
			}
		})
	}
}

func TestCheckerAllowsAppendingTestedClient(t *testing.T) {
	repo := newFixture(t, "v0.1.0")
	writeFile(t, repo, "releases/services/alpha.yaml", `service: alpha
releases:
  - version: v0.1.0
    contract: gen/go/alpha/v0.1.0
    tested_clients:
      - pkg/alpha/v0.1.0
`)
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-m", "record service release")
	git(t, repo, "tag", "service/alpha/v0.1.0")

	writeFile(t, repo, "go.mod", rootGoMod("v0.1.1"))
	writeFile(t, repo, "pkg/alpha/client.go", "package alpha\n\nconst revision = 2\n")
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-m", "release client patch")
	git(t, repo, "tag", "pkg/alpha/v0.1.1")
	writeFile(t, repo, "releases/services/alpha.yaml", `service: alpha
releases:
  - version: v0.1.0
    contract: gen/go/alpha/v0.1.0
    tested_clients:
      - pkg/alpha/v0.1.0
      - pkg/alpha/v0.1.1
`)
	if err := (&checker{repo: repo, skipBuild: true}).run("alpha", "v0.1.0"); err != nil {
		t.Fatal(err)
	}
}

func TestCheckerRejectsHistoricalMappingChanges(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		manifest string
		contains string
	}{
		{
			name: "remove client",
			manifest: `service: alpha
releases:
  - version: v0.1.0
    contract: gen/go/alpha/v0.1.0
    tested_clients: []
`,
			contains: "cannot be removed",
		},
		{
			name: "change contract",
			manifest: `service: alpha
releases:
  - version: v0.1.0
`,
			contains: "contract for service/alpha/v0.1.0 is immutable",
		},
		{
			name:    "remove release",
			version: "v0.2.0",
			manifest: `service: alpha
releases:
  - version: v0.2.0
`,
			contains: "released service service/alpha/v0.1.0 cannot be removed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := newFixture(t, "v0.1.0")
			writeFile(t, repo, "releases/services/alpha.yaml", `service: alpha
releases:
  - version: v0.1.0
    contract: gen/go/alpha/v0.1.0
    tested_clients:
      - pkg/alpha/v0.1.0
`)
			git(t, repo, "add", ".")
			git(t, repo, "commit", "-m", "record service release")
			git(t, repo, "tag", "service/alpha/v0.1.0")
			writeFile(t, repo, "releases/services/alpha.yaml", test.manifest)
			version := test.version
			if version == "" {
				version = "v0.1.0"
			}
			err := (&checker{repo: repo, skipBuild: true}).run("alpha", version)
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("error = %v, want substring %q", err, test.contains)
			}
		})
	}
}

func TestNormalizeModuleFileRemovesLicenseHeader(t *testing.T) {
	old := []byte("package sequence\n\nconst value = 1\n")
	current := []byte(`// Copyright 2026 Codesjoy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// limitations under the License.

package sequence

const value = 1
`)
	if got, want := string(
		normalizeModuleFile("pkg/sequence/file.go", current),
	), string(
		old,
	); got != want {
		t.Fatalf("normalized file = %q, want %q", got, want)
	}
}

func newFixture(t *testing.T, clientVersion string) string {
	t.Helper()
	repo := t.TempDir()
	for _, path := range []string{"cmd/alpha", "internal/alpha", "gen/go/alpha", "pkg/alpha", "releases/services"} {
		if err := os.MkdirAll(filepath.Join(repo, path), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	git(t, repo, "init", "-b", "main")
	git(t, repo, "config", "user.name", "release test")
	git(t, repo, "config", "user.email", "release@example.com")
	writeFile(t, repo, "go.mod", rootGoMod(clientVersion))
	writeFile(t, repo, "cmd/alpha/main.go", "package main\n\nfunc main() {}\n")
	writeFile(t, repo, "internal/alpha/alpha.go", "package alpha\n")
	writeFile(
		t,
		repo,
		"gen/go/alpha/go.mod",
		"module github.com/codesjoy/sindri/gen/go/alpha\n\ngo 1.26.4\n",
	)
	writeFile(t, repo, "gen/go/alpha/contract.go", "package alpha\n")
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-m", "release contract")
	git(t, repo, "tag", "gen/go/alpha/v0.1.0")
	writeFile(t, repo, "pkg/alpha/go.mod", `module github.com/codesjoy/sindri/pkg/alpha

go 1.26.4

require github.com/codesjoy/sindri/gen/go/alpha v0.1.0
`)
	writeFile(t, repo, "pkg/alpha/client.go", "package alpha\n")
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-m", "release client")
	git(t, repo, "tag", "pkg/alpha/v0.1.0")
	return repo
}

func rootGoMod(clientVersion string) string {
	return `module github.com/codesjoy/sindri

go 1.26.4

require (
	github.com/codesjoy/sindri/gen/go/alpha v0.1.0
	github.com/codesjoy/sindri/pkg/alpha ` + clientVersion + `
)
`
}

func writeFile(t *testing.T, root, path, content string) {
	t.Helper()
	fullPath := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func git(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "git", args...)
	cmd.Dir = repo
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %s: %v", strings.Join(args, " "), output, err)
	}
	return strings.TrimSpace(string(output))
}
