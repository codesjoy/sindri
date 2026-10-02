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

//go:build integration

package sequence_test

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// readinessReport is the readiness endpoint's body.
//
// It is declared here rather than shared with the process, so a change to the
// endpoint that a probe would have to follow shows up as a failing test rather
// than as a silently updated struct. The data and control halves are pointers
// because a probe only carries the halves the process was told to run.
type readinessReport struct {
	Ready   bool              `json:"ready"`
	Reason  string            `json:"reason"`
	Data    *dataReadiness    `json:"data"`
	Control *controlReadiness `json:"control"`
}

// dataReadiness is the data plane's half of the report.
type dataReadiness struct {
	Ready                bool    `json:"ready"`
	Reason               string  `json:"reason"`
	QuietWindowSeconds   float64 `json:"quiet_window_seconds"`
	LeaseDurationSeconds float64 `json:"lease_duration_seconds"`
	MaxPauseSeconds      float64 `json:"max_pause_seconds"`
}

// controlReadiness is the publisher's half of the report.
type controlReadiness struct {
	Ready          bool   `json:"ready"`
	Reason         string `json:"reason"`
	LastPassFailed bool   `json:"last_pass_failed"`
	Passes         int64  `json:"passes"`
	UnownedSlots   int64  `json:"unowned_slots"`
	Revision       int64  `json:"revision"`
}

// probeReadiness reads a readiness endpoint.
//
// It returns the error instead of failing so it can be called from a polling
// condition, which runs on another goroutine and must not stop the test.
func probeReadiness(url string) (int, readinessReport, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		return 0, readinessReport{}, err
	}
	defer func() { _ = response.Body.Close() }()
	var report readinessReport
	if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
		return response.StatusCode, readinessReport{}, err
	}
	return response.StatusCode, report, nil
}

// TestReadinessStaysServingThroughAStorageOutage is the section D.17 rule
// against a real outage.
//
// Losing storage makes this instance stop handing out ids, which looks like a
// reason to take it out of service. It is not: a restart cannot repair the
// storage, and reporting the instance unready would fire a rolling restart of
// every replica against the same outage. The protocol already fails the
// affected requests closed with their own retriable reason, so the probe stays
// green and the outage is handled where it actually is.
func (s *SequenceSystemSuite) TestReadinessStaysServingThroughAStorageOutage() {
	key := "readiness-storage-outage"
	version := s.publishRoute(allSlots("node-a"))
	s.waitForRoute("node-a", version)
	s.waitForOwnership("node-a", key, version)

	s.Require().NoError(s.proxies["db-a"].Disable())
	defer func() {
		if err := s.proxies["db-a"].Enable(); err != nil {
			s.T().Errorf("enable db-a proxy: %v", err)
		}
	}()

	// Wait until the outage has actually been noticed, so the probe is read
	// while the instance is degraded rather than before it finds out.
	s.Require().Eventually(func() bool {
		callCtx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		_, err := s.fetchDirect(callCtx, "node-a", key, version)
		return refusedWhileStorageUnavailable(err)
	}, recoveryDeadline, 50*time.Millisecond)

	status, report, err := probeReadiness(s.nodes["node-a"].readyAddr)
	s.Require().NoError(err)
	s.Equal(http.StatusOK, status)
	s.True(report.Ready)
	s.Equal("serving", report.Reason)
}
