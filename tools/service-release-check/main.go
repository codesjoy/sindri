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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

var semverPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)

type manifest struct {
	Service  string    `yaml:"service"`
	Releases []release `yaml:"releases"`
}

type release struct {
	Version       string   `yaml:"version"`
	Contract      string   `yaml:"contract"`
	TestedClients []string `yaml:"tested_clients"`
}

type checker struct {
	repo      string
	skipBuild bool
}

func main() {
	service := flag.String("service", "", "service name")
	version := flag.String("version", "", "service release version")
	repo := flag.String("repo", ".", "repository root")
	flag.Parse()
	if err := (&checker{repo: *repo}).run(*service, *version); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("service release checks passed for %s %s\n", *service, *version)
}

func (c *checker) run(service, version string) error {
	if !regexp.MustCompile(`^[a-z][a-z0-9]*$`).MatchString(service) {
		return fmt.Errorf("invalid service name %q", service)
	}
	if !semverPattern.MatchString(version) {
		return fmt.Errorf("invalid semantic version %q", version)
	}
	for _, path := range []string{
		filepath.Join(c.repo, "cmd", service),
		filepath.Join(c.repo, "internal", service),
	} {
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			return fmt.Errorf("missing service directory %s", path)
		}
	}

	data, err := os.ReadFile(filepath.Join(c.repo, "releases", "services", service+".yaml"))
	if err != nil {
		return fmt.Errorf("read release manifest: %w", err)
	}
	doc, err := decodeManifest(data)
	if err != nil {
		return fmt.Errorf("parse release manifest: %w", err)
	}
	if doc.Service != service {
		return fmt.Errorf("manifest service %q does not match %q", doc.Service, service)
	}
	var entry *release
	seenVersions := make(map[string]struct{}, len(doc.Releases))
	for i := range doc.Releases {
		candidate := doc.Releases[i]
		if !semverPattern.MatchString(candidate.Version) {
			return fmt.Errorf("manifest contains invalid release version %q", candidate.Version)
		}
		if _, ok := seenVersions[candidate.Version]; ok {
			return fmt.Errorf("manifest contains duplicate release %s", candidate.Version)
		}
		seenVersions[candidate.Version] = struct{}{}
		if err := validateManifestEntry(candidate); err != nil {
			return fmt.Errorf("release %s: %w", candidate.Version, err)
		}
		if doc.Releases[i].Version == version {
			entry = &doc.Releases[i]
		}
	}
	if entry == nil {
		return fmt.Errorf("manifest has no release %s", version)
	}
	if entry.Contract != "" && !strings.HasPrefix(entry.Contract, "gen/go/"+service+"/") {
		return fmt.Errorf("contract tag %s does not belong to service %s", entry.Contract, service)
	}
	if err := c.validateTags(*entry); err != nil {
		return err
	}
	selectedClient, err := c.validateDependencies(*entry)
	if err != nil {
		return err
	}
	if err := c.validateHistory(service, doc); err != nil {
		return err
	}
	if err := c.validateModuleTrees(*entry, selectedClient); err != nil {
		return err
	}
	if c.skipBuild {
		return nil
	}
	return c.validateServiceBuild(service)
}

func decodeManifest(data []byte) (manifest, error) {
	var doc manifest
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil {
		return manifest{}, err
	}
	return doc, nil
}

func validateManifestEntry(entry release) error {
	if entry.Contract == "" && len(entry.TestedClients) != 0 {
		return errors.New("tested clients require a contract")
	}
	if entry.Contract != "" && !validModuleTag(entry.Contract, "gen/go/") {
		return fmt.Errorf("invalid contract tag %q", entry.Contract)
	}
	seen := make(map[string]struct{}, len(entry.TestedClients))
	for _, client := range entry.TestedClients {
		if !validModuleTag(client, "pkg/") {
			return fmt.Errorf("invalid client tag %q", client)
		}
		if _, ok := seen[client]; ok {
			return fmt.Errorf("duplicate client tag %q", client)
		}
		seen[client] = struct{}{}
	}
	return nil
}

func validModuleTag(tag, prefix string) bool {
	if !strings.HasPrefix(tag, prefix) {
		return false
	}
	idx := strings.LastIndex(tag, "/v")
	return idx > len(prefix) && semverPattern.MatchString(tag[idx+1:])
}

func (c *checker) validateTags(entry release) error {
	tags := append([]string{}, entry.TestedClients...)
	if entry.Contract != "" {
		tags = append(tags, entry.Contract)
	}
	for _, tag := range tags {
		if _, err := c.git("rev-parse", "--verify", "refs/tags/"+tag+"^{commit}"); err != nil {
			return fmt.Errorf("missing module tag %s", tag)
		}
	}
	return nil
}

func (c *checker) validateDependencies(entry release) (string, error) {
	rootMod, err := c.goModJSON(filepath.Join(c.repo, "go.mod"))
	if err != nil {
		return "", fmt.Errorf("parse root go.mod: %w", err)
	}
	if entry.Contract != "" {
		contractPath, contractVersion := tagModule(entry.Contract)
		if rootMod[contractPath] != contractVersion {
			return "", fmt.Errorf(
				"root go.mod requires %s %s, got %q",
				contractPath,
				contractVersion,
				rootMod[contractPath],
			)
		}
		for _, client := range entry.TestedClients {
			modData, err := c.gitRaw("show", client+":"+tagDirectory(client)+"/go.mod")
			if err != nil {
				return "", fmt.Errorf("read %s go.mod: %w", client, err)
			}
			clientMod, err := c.goModData(modData)
			if err != nil {
				return "", fmt.Errorf("parse %s go.mod: %w", client, err)
			}
			if clientMod[contractPath] != contractVersion {
				return "", fmt.Errorf(
					"%s requires %s %q, want %s",
					client,
					contractPath,
					clientMod[contractPath],
					contractVersion,
				)
			}
		}
	}
	selectedClient := ""
	for _, client := range entry.TestedClients {
		clientPath, clientVersion := tagModule(client)
		if rootMod[clientPath] == clientVersion {
			selectedClient = client
			break
		}
	}
	if len(entry.TestedClients) != 0 && selectedClient == "" {
		return "", errors.New("root go.mod does not select any tested client version")
	}
	return selectedClient, nil
}

func (c *checker) validateModuleTrees(entry release, selectedClient string) error {
	for _, tag := range []string{entry.Contract, selectedClient} {
		if tag == "" {
			continue
		}
		if err := c.compareModuleTree(tag); err != nil {
			return err
		}
	}
	return nil
}

func (c *checker) validateHistory(service string, current manifest) error {
	currentVersions := make(map[string]struct{}, len(current.Releases))
	for _, currentRelease := range current.Releases {
		currentVersions[currentRelease.Version] = struct{}{}
	}
	serviceTags, err := c.gitLines("tag", "--list", "service/"+service+"/v*")
	if err != nil {
		return fmt.Errorf("list service tags: %w", err)
	}
	for _, serviceTag := range serviceTags {
		version := strings.TrimPrefix(serviceTag, "service/"+service+"/")
		if _, ok := currentVersions[version]; !ok {
			return fmt.Errorf("released service %s cannot be removed from the manifest", serviceTag)
		}
	}
	for _, currentRelease := range current.Releases {
		serviceTag := "service/" + service + "/" + currentRelease.Version
		if _, err := c.git(
			"rev-parse",
			"--verify",
			"refs/tags/"+serviceTag+"^{commit}",
		); err != nil {
			continue
		}
		data, err := c.git("show", serviceTag+":releases/services/"+service+".yaml")
		if err != nil {
			return fmt.Errorf("read historical manifest at %s: %w", serviceTag, err)
		}
		old, err := decodeManifest([]byte(data))
		if err != nil {
			return fmt.Errorf("parse historical manifest at %s: %w", serviceTag, err)
		}
		var previous *release
		for i := range old.Releases {
			if old.Releases[i].Version == currentRelease.Version {
				previous = &old.Releases[i]
				break
			}
		}
		if previous == nil {
			return fmt.Errorf(
				"historical manifest at %s has no release %s",
				serviceTag,
				currentRelease.Version,
			)
		}
		if previous.Contract != currentRelease.Contract {
			return fmt.Errorf("contract for %s is immutable", serviceTag)
		}
		currentClients := make(map[string]struct{}, len(currentRelease.TestedClients))
		for _, client := range currentRelease.TestedClients {
			currentClients[client] = struct{}{}
		}
		for _, client := range previous.TestedClients {
			if _, ok := currentClients[client]; !ok {
				return fmt.Errorf("tested client %s cannot be removed from %s", client, serviceTag)
			}
		}
	}
	return nil
}

func (c *checker) validateServiceBuild(service string) error {
	tmp, err := os.MkdirTemp(c.repo, ".sindri-service-release-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	modfile := filepath.Join(tmp, "go.mod")
	data, err := os.ReadFile(filepath.Join(c.repo, "go.mod"))
	if err != nil {
		return err
	}
	if err := os.WriteFile(modfile, data, 0o600); err != nil {
		return err
	}
	parsed, err := c.goModEditJSON(modfile)
	if err != nil {
		return err
	}
	for _, replacement := range parsed.Replace {
		cmd := exec.CommandContext(
			context.Background(),
			"go",
			"mod",
			"edit",
			"-modfile="+modfile,
			"-dropreplace="+replacement.Old.Path,
		)
		cmd.Dir = c.repo
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf(
				"remove local replacement for %s: %s: %w",
				replacement.Old.Path,
				output,
				err,
			)
		}
	}
	cmd := exec.CommandContext(
		context.Background(),
		"go",
		"test",
		"-mod=mod",
		"-modfile="+modfile,
		"./cmd/"+service,
		"./internal/"+service+"/...",
	)
	cmd.Dir = c.repo
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build service with published modules:\n%s\n%w", output, err)
	}
	return nil
}

func tagModule(tag string) (string, string) {
	idx := strings.LastIndex(tag, "/v")
	return "github.com/codesjoy/sindri/" + tag[:idx], tag[idx+1:]
}

func tagDirectory(tag string) string {
	idx := strings.LastIndex(tag, "/v")
	return tag[:idx]
}

func (c *checker) compareModuleTree(tag string) error {
	moduleDir := tagDirectory(tag)
	files, err := c.gitLines("ls-tree", "-r", "--name-only", tag, "--", moduleDir)
	if err != nil {
		return fmt.Errorf("list %s at %s: %w", moduleDir, tag, err)
	}
	want := make(map[string]struct{}, len(files))
	for _, path := range files {
		want[path] = struct{}{}
	}
	var got []string
	err = filepath.Walk(
		filepath.Join(c.repo, moduleDir),
		func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !info.IsDir() {
				rel, err := filepath.Rel(c.repo, path)
				if err != nil {
					return err
				}
				got = append(got, filepath.ToSlash(rel))
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("walk %s: %w", moduleDir, err)
	}
	sort.Strings(got)
	if len(got) != len(want) {
		return fmt.Errorf("module tree differs from %s: file count changed", tag)
	}
	for _, path := range got {
		if _, ok := want[path]; !ok {
			return fmt.Errorf("module tree differs from %s: unexpected file %s", tag, path)
		}
		old, err := c.gitRaw("show", tag+":"+path)
		if err != nil {
			return fmt.Errorf("read %s at %s: %w", path, tag, err)
		}
		current, err := os.ReadFile(filepath.Join(c.repo, filepath.FromSlash(path)))
		if err != nil {
			return fmt.Errorf("read current %s: %w", path, err)
		}
		if !bytes.Equal(normalizeModuleFile(path, old), normalizeModuleFile(path, current)) {
			return fmt.Errorf("module tree differs from %s: %s", tag, path)
		}
	}
	return nil
}

func normalizeModuleFile(path string, data []byte) []byte {
	if !strings.HasSuffix(path, ".go") {
		return data
	}
	lines := strings.SplitAfter(string(data), "\n")
	if len(lines) < 2 || !strings.HasPrefix(lines[0], "// Copyright ") {
		return data
	}
	licenseFound := false
	for i, line := range lines {
		if strings.Contains(line, "Licensed under the Apache License, Version 2.0") {
			licenseFound = true
		}
		if strings.TrimSpace(line) == "// limitations under the License." && i+1 < len(lines) {
			if !licenseFound {
				return data
			}
			remaining := lines[i+1:]
			if len(remaining) != 0 && strings.TrimSpace(remaining[0]) == "" {
				remaining = remaining[1:]
			}
			return []byte(strings.Join(remaining, ""))
		}
	}
	return data
}

func (c *checker) git(args ...string) (string, error) {
	output, err := c.gitRaw(args...)
	return strings.TrimSpace(string(output)), err
}

func (c *checker) gitRaw(args ...string) ([]byte, error) {
	cmd := exec.CommandContext(context.Background(), "git", args...)
	cmd.Dir = c.repo
	return cmd.Output()
}

func (c *checker) gitLines(args ...string) ([]string, error) {
	out, err := c.git(args...)
	if err != nil || out == "" {
		return nil, err
	}
	return strings.Split(out, "\n"), nil
}

func (c *checker) goModJSON(path string) (map[string]string, error) {
	parsed, err := c.goModEditJSON(path)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(parsed.Require))
	for _, req := range parsed.Require {
		result[req.Path] = req.Version
	}
	return result, nil
}

type goModEdit struct {
	Require []struct {
		Path    string
		Version string
	} `json:"Require"`
	Replace []struct {
		Old struct {
			Path string
		} `json:"Old"`
	} `json:"Replace"`
}

func (c *checker) goModEditJSON(path string) (goModEdit, error) {
	cmd := exec.CommandContext(
		context.Background(),
		"go",
		"mod",
		"edit",
		"-json",
		"-modfile="+path,
	)
	cmd.Dir = c.repo
	out, err := cmd.Output()
	if err != nil {
		return goModEdit{}, err
	}
	var parsed goModEdit
	if err := json.Unmarshal(out, &parsed); err != nil {
		return goModEdit{}, err
	}
	return parsed, nil
}

func (c *checker) goModData(data []byte) (map[string]string, error) {
	tmp, err := os.CreateTemp("", "sindri-release-*.mod")
	if err != nil {
		return nil, err
	}
	path := tmp.Name()
	defer func() { _ = os.Remove(path) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	return c.goModJSON(path)
}
