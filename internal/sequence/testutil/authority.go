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

// Package testutil provides authority fixtures for adapter tests. Production
// always uses database authority; these fixtures never disable its protocol.
package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
)

// Authority is an in-memory OwnershipRepo fixture that still enforces the
// revision and epoch protocol the durable store enforces.
type Authority struct {
	mu     sync.Mutex
	leases map[string]biz.InstanceLease
	slots  map[uint32]biz.Ownership
	Epoch  uint64
}

// NewAuthority builds an empty authority fixture.
func NewAuthority() *Authority {
	return &Authority{
		leases: map[string]biz.InstanceLease{},
		slots:  map[uint32]biz.Ownership{},
		Epoch:  1,
	}
}

// RegisterInstance inserts the instance if absent and rejects a conflict.
func (f *Authority) RegisterInstance(_ context.Context, node, instance string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l, ok := f.leases[instance]; ok {
		if l.NodeID != node || l.State != "ACTIVE" {
			return biz.ErrAuthorityChanged
		}
		return nil
	}
	f.leases[instance] = biz.InstanceLease{
		NodeID:     node,
		InstanceID: instance,
		State:      "ACTIVE",
		GrantedAt:  time.Now(),
	}
	return nil
}

// RenewInstance moves the grant forward when the revision matches.
func (f *Authority) RenewInstance(
	_ context.Context,
	instance string,
	revision uint64,
) (biz.InstanceLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.leases[instance]
	if l.State != "ACTIVE" || l.Revision != revision {
		return biz.InstanceLease{}, biz.ErrAuthorityChanged
	}
	l.GrantedAt = time.Now()
	f.leases[instance] = l
	return l, nil
}

// InstanceAuthority returns the instance's lease and the slots it owns.
func (f *Authority) InstanceAuthority(
	_ context.Context,
	instance string,
) (biz.AuthoritySnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.leases[instance]
	if l.State != "ACTIVE" {
		return biz.AuthoritySnapshot{}, biz.ErrAuthorityChanged
	}
	s := biz.AuthoritySnapshot{Lease: l}
	for _, o := range f.slots {
		if o.OwnerInstanceID == instance {
			s.Slots = append(s.Slots, o)
		}
	}
	return s, nil
}

// RetireInstance marks the instance RETIRED and advances its revision.
func (f *Authority) RetireInstance(_ context.Context, instance string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.leases[instance]
	l.State = "RETIRED"
	l.Revision++
	f.leases[instance] = l
	return nil
}

// StorageClock returns the fixture's local clock.
func (f *Authority) StorageClock(context.Context) (time.Time, error) { return time.Now(), nil }

// ClaimSlots grants the request's slots under the revision and quiet-window
// rules the durable store applies.
func (f *Authority) ClaimSlots(
	_ context.Context,
	req biz.ClaimRequest,
) ([]biz.ClaimOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.leases[req.InstanceID]
	if l.State != "ACTIVE" || l.Revision != req.Revision {
		return nil, biz.ErrAuthorityChanged
	}
	var result []biz.ClaimOutcome
	changed := false
	for _, id := range req.Slots {
		o := f.slots[id]
		if o.OwnerInstanceID != "" && o.OwnerInstanceID != req.InstanceID {
			result = append(result, biz.ClaimOutcome{Ownership: o})
			continue
		}
		if o.OwnerInstanceID == "" {
			o = biz.Ownership{
				SlotID:          id,
				OwnerNodeID:     req.NodeID,
				OwnerInstanceID: req.InstanceID,
				State:           biz.SlotOwned,
				Epoch:           max(f.Epoch, o.Epoch+1),
			}
			f.slots[id] = o
			changed = true
		}
		result = append(result, biz.ClaimOutcome{Ownership: o, Granted: true})
	}
	if changed {
		l.Revision++
		f.leases[req.InstanceID] = l
	}
	return result, nil
}
