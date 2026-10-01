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

package service

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
)

// ReadinessPath is the raw HTTP route a readiness probe reads.
//
// It is exported because it is part of what a deployment configures: a probe
// definition outside this repository names it, so the string cannot be free to
// drift between here and there.
const ReadinessPath = "/healthz"

// ReadinessBounds carries the HA bounds the data-plane report publishes.
type ReadinessBounds struct {
	QuietWindow   time.Duration
	LeaseDuration time.Duration
	MaxPause      time.Duration
}

// readinessBody is the probe's answer, in JSON.
//
// The verdict is the conjunction of the halves this process was told to run, and
// each half is also reported on its own. A single boolean would answer "should
// this process be restarted", but an operator looking at a combined process
// needs to know which half is unready before deciding what to do about it.
type readinessBody struct {
	Ready  bool   `json:"ready"`
	Reason string `json:"reason"`
	// Data is present when this process runs the data plane.
	Data *dataReadinessBody `json:"data,omitempty"`
	// Control is present when this process runs the publisher.
	Control *controlReadinessBody `json:"control,omitempty"`
}

// dataReadinessBody is the data plane's report.
//
// It carries the bounds in force next to the verdict because the verdict alone
// cannot be acted on safely. An operator who sees a process report ready needs
// to know which bounds the ordering argument rests on, and an operator who sees
// it unready needs the state name rather than a bare status code.
type dataReadinessBody struct {
	Ready  bool   `json:"ready"`
	Reason string `json:"reason"`
	// QuietWindowSeconds is W, in seconds, the silent-takeover window.
	QuietWindowSeconds float64 `json:"quiet_window_seconds"`
	// LeaseDurationSeconds is L, in seconds, the local lease every held slot is
	// armed with. An owner serves from it for at most L without a confirmed
	// renewal, so it is the length of the interval a takeover has to cover.
	LeaseDurationSeconds float64 `json:"lease_duration_seconds"`
	// MaxPauseSeconds is P_max, in seconds, the process pause bound the platform
	// was asserted to provide. It enters the quiet window's lower bound.
	MaxPauseSeconds float64 `json:"max_pause_seconds"`
}

// controlReadinessBody is the publisher's report.
//
// It carries the counters next to the verdict because this half holds no issuing
// authority: whether it should be restarted depends on whether its loop has
// stopped or its passes are failing, and on how much of the fleet is still
// unowned.
type controlReadinessBody struct {
	Ready  bool   `json:"ready"`
	Reason string `json:"reason"`
	// LastPassFailed says the most recent pass returned an error. It is reported
	// next to the verdict rather than folded into it, because it usually means
	// storage is unreachable -- which a restart does not fix.
	LastPassFailed bool `json:"last_pass_failed"`
	// Passes counts completed passes since this replica started.
	Passes int64 `json:"passes"`
	// UnownedSlots is the last pass's count of slots nobody holds.
	UnownedSlots int64 `json:"unowned_slots"`
	// Revision is the newest directory revision this replica published.
	Revision int64 `json:"revision"`
}

// NewReadinessHandler answers the readiness probe for the halves this process
// runs. A nil data or control dependency means that half is not part of the
// deployment and is omitted from the report.
//
// The status code is the machine-readable half and the body is the human half,
// so the verdict is not duplicated between them: any state a probe should fail
// gets a non-200, and the reason it failed is carried in both.
func NewReadinessHandler(
	allocator *biz.Allocator,
	publisher *biz.Publisher,
	bounds ReadinessBounds,
) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		body := readinessBody{}
		if allocator != nil {
			readiness := allocator.Readiness()
			body.Data = &dataReadinessBody{
				Ready:                readiness.Ready,
				Reason:               readiness.Reason,
				QuietWindowSeconds:   bounds.QuietWindow.Seconds(),
				LeaseDurationSeconds: bounds.LeaseDuration.Seconds(),
				MaxPauseSeconds:      bounds.MaxPause.Seconds(),
			}
		}
		if publisher != nil {
			readiness := publisher.Readiness()
			stats := publisher.Stats()
			body.Control = &controlReadinessBody{
				Ready:          readiness.Ready,
				Reason:         readiness.Reason,
				LastPassFailed: readiness.LastPassFailed,
				Passes:         stats.Passes,
				UnownedSlots:   stats.UnownedSlots,
				Revision:       stats.Revision,
			}
		}
		body.Ready, body.Reason = combinedReadiness(body)

		payload, err := json.Marshal(body)
		if err != nil {
			// The body is a fixed struct of numbers and strings, so this is
			// unreachable; answering 500 rather than a truncated body keeps the
			// probe honest if it ever stops being unreachable.
			http.Error(w, "sequence readiness unavailable", http.StatusInternalServerError)
			return
		}
		status := http.StatusOK
		if !body.Ready {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(status)
		_, _ = w.Write(payload)
	}
}

// combinedReadiness folds the halves into one verdict.
//
// Both halves have to be ready. The data plane decides whether this process can
// serve, and the publisher decides whether a client can find out where; a
// combined process that has stopped publishing keeps answering from the last
// directory, which is exactly the failure a probe has to surface.
func combinedReadiness(body readinessBody) (bool, string) {
	switch {
	case body.Data != nil && !body.Data.Ready:
		return false, body.Data.Reason
	case body.Control != nil && !body.Control.Ready:
		return false, body.Control.Reason
	default:
		return true, "serving"
	}
}
