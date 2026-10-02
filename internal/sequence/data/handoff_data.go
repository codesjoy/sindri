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

package data

import (
	"context"
	"errors"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type handoffRow struct {
	SlotID           uint32     `gorm:"column:slot_id;primaryKey;autoIncrement:false"`
	ID               string     `gorm:"column:handoff_id"`
	Kind             string     `gorm:"column:kind"`
	SourceInstanceID string     `gorm:"column:source_instance_id"`
	SourceEpoch      uint64     `gorm:"column:source_epoch"`
	TargetInstanceID *string    `gorm:"column:target_instance_id"`
	Phase            string     `gorm:"column:phase"`
	Drained          bool       `gorm:"column:drained"`
	TargetReady      bool       `gorm:"column:target_ready"`
	NotBefore        *time.Time `gorm:"column:not_before"`
	CreatedAt        time.Time  `gorm:"column:created_at"`
	UpdatedAt        time.Time  `gorm:"column:updated_at"`
}

func (handoffRow) TableName() string { return "sequence_slot_handoffs" }
func (r handoffRow) handoff() biz.Handoff {
	h := biz.Handoff{
		SlotID:           r.SlotID,
		ID:               r.ID,
		Kind:             r.Kind,
		SourceInstanceID: r.SourceInstanceID,
		SourceEpoch:      r.SourceEpoch,
		Phase:            r.Phase,
		Drained:          r.Drained,
		TargetReady:      r.TargetReady,
	}
	if r.TargetInstanceID != nil {
		h.TargetInstanceID = *r.TargetInstanceID
	}
	if r.NotBefore != nil {
		h.NotBefore = *r.NotBefore
	}
	return h
}

func rowForHandoff(h biz.Handoff, now time.Time) handoffRow {
	r := handoffRow{
		SlotID:           h.SlotID,
		ID:               h.ID,
		Kind:             h.Kind,
		SourceInstanceID: h.SourceInstanceID,
		SourceEpoch:      h.SourceEpoch,
		Phase:            h.Phase,
		Drained:          h.Drained,
		TargetReady:      h.TargetReady,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if h.TargetInstanceID != "" {
		target := h.TargetInstanceID
		r.TargetInstanceID = &target
	}
	if !h.NotBefore.IsZero() {
		boundary := h.NotBefore
		r.NotBefore = &boundary
	}
	return r
}

type handoffData struct{ db *gorm.DB }

// NewHandoffData builds the durable handoff state machine over db.
func NewHandoffData(db *gorm.DB) biz.HandoffRepo { return &handoffData{db: db} }

func (d *handoffData) Handoffs(
	ctx context.Context,
	instanceID string,
	limit int,
) ([]biz.Handoff, error) {
	if limit <= 0 || limit > biz.SlotCount {
		return nil, errors.New("sequence: invalid handoff read bound")
	}
	query := d.db.WithContext(ctx).Where("phase NOT IN ('COMPLETED', 'CANCELLED')")
	if instanceID != "" {
		query = query.Where(
			"source_instance_id = ? OR target_instance_id = ?",
			instanceID,
			instanceID,
		)
	}
	var rows []handoffRow
	if err := query.Order("slot_id").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}
	result := make([]biz.Handoff, 0, len(rows))
	for _, r := range rows {
		result = append(result, r.handoff())
	}
	return result, nil
}

func lockControlRow(ctx context.Context, tx *gorm.DB) error {
	var row coordinatorRow
	return tx.WithContext(ctx).
		Table("sequence_coordinator").
		Where("id = 1").
		Clauses(lockClause(tx, "UPDATE")).
		Take(&row).
		Error
}

func lockedHandoffs(
	ctx context.Context,
	tx *gorm.DB,
	slots []uint32,
) (map[uint32]handoffRow, error) {
	var rows []handoffRow
	if err := tx.WithContext(ctx).
		Where("slot_id IN ?", slots).
		Order("slot_id").
		Clauses(lockClause(tx, "UPDATE")).
		Find(&rows).
		Error; err != nil {
		return nil, err
	}
	result := make(map[uint32]handoffRow, len(rows))
	for _, r := range rows {
		result[r.SlotID] = r
	}
	return result, nil
}

func writeHandoffs(tx *gorm.DB, rows []handoffRow) error {
	if len(rows) == 0 {
		return nil
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "slot_id"}}, DoUpdates: clause.AssignmentColumns([]string{"handoff_id", "kind", "source_instance_id", "source_epoch", "target_instance_id", "phase", "drained", "target_ready", "not_before", "created_at", "updated_at"})}).
		Create(&rows).
		Error
}

func freshInstance(r instanceRow, now time.Time, lease time.Duration) bool {
	return r.InstanceID != "" && r.State == "ACTIVE" && lease > 0 &&
		now.Before(r.GrantedAt.Add(lease))
}

func (d *handoffData) PlanHandoffs(
	ctx context.Context,
	plans []biz.Handoff,
	tenure biz.CoordinatorLease,
	limits biz.MigrationConfig,
	lease time.Duration,
) error {
	if len(plans) == 0 {
		return nil
	}
	if len(plans) > limits.BatchSlots {
		return errors.New("sequence: handoff batch exceeds limit")
	}
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := GuardCoordinator(ctx, tx, tx.Name(), tenure); err != nil {
			return err
		}
		var pending int64
		if err := tx.Model(&handoffRow{}).
			Where("phase NOT IN ('COMPLETED', 'CANCELLED')").
			Count(&pending).
			Error; err != nil {
			return err
		}
		if pending+int64(len(plans)) > int64(limits.MaxPlanned) {
			return nil
		}
		var ids []string
		var slots []uint32
		for _, h := range plans {
			if h.ID == "" || h.Kind != "TRANSFER" || h.SourceInstanceID == h.TargetInstanceID ||
				h.SourceEpoch == 0 ||
				h.Phase != "PLANNED" {
				return errors.New("sequence: invalid migration intent")
			}
			ids = append(ids, h.SourceInstanceID, h.TargetInstanceID)
			slots = append(slots, h.SlotID)
		}
		leases, err := lockInstances(ctx, tx, ids, "UPDATE")
		if err != nil {
			return err
		}
		ownership, err := loadSlots(ctx, tx, slots, "UPDATE")
		if err != nil {
			return err
		}
		bySlot := make(map[uint32]ownershipRow, len(ownership))
		for _, r := range ownership {
			bySlot[r.SlotID] = r
		}
		existing, err := lockedHandoffs(ctx, tx, slots)
		if err != nil {
			return err
		}
		now, err := readStorageClock(ctx, tx, tx.Name())
		if err != nil {
			return err
		}
		var rows []handoffRow
		for _, h := range plans {
			o := bySlot[h.SlotID]
			if o.OwnerInstanceID == nil || *o.OwnerInstanceID != h.SourceInstanceID ||
				o.Epoch != h.SourceEpoch ||
				o.State != string(biz.SlotOwned) ||
				!freshInstance(leases[h.TargetInstanceID], now, lease) ||
				!freshInstance(leases[h.SourceInstanceID], now, lease) {
				continue
			}
			if current, ok := existing[h.SlotID]; ok && current.handoff().Active() {
				continue
			}
			rows = append(rows, rowForHandoff(h, now))
		}
		return writeHandoffs(tx, rows)
	})
}

func (d *handoffData) ApplyHandoffs(ctx context.Context, request biz.HandoffRequest) error {
	if len(request.Commands) == 0 {
		return nil
	}
	if len(request.Commands) > request.Limits.BatchSlots || request.InstanceID == "" {
		return errors.New("sequence: invalid handoff execution batch")
	}
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// This row serializes admission budgets, not execution authority; existing intents survive coordinator outages.
		if err := lockControlRow(ctx, tx); err != nil {
			return err
		}
		ids := []string{request.InstanceID}
		var slots []uint32
		for _, c := range request.Commands {
			ids = append(ids, c.Handoff.SourceInstanceID, c.Handoff.TargetInstanceID)
			slots = append(slots, c.Handoff.SlotID)
		}
		leases, err := lockInstances(ctx, tx, ids, "UPDATE")
		if err != nil {
			return err
		}
		ownership, err := loadSlots(ctx, tx, slots, "UPDATE")
		if err != nil {
			return err
		}
		owned := make(map[uint32]ownershipRow, len(ownership))
		for _, r := range ownership {
			owned[r.SlotID] = r
		}
		current, err := lockedHandoffs(ctx, tx, slots)
		if err != nil {
			return err
		}
		var inflight []handoffRow
		if err := tx.Where("phase IN ('DRAINING', 'TRANSFERRED')").
			Find(&inflight).
			Error; err != nil {
			return err
		}
		outgoing, incoming := make(map[string]int), make(map[string]int)
		for _, r := range inflight {
			outgoing[r.SourceInstanceID]++
			if r.TargetInstanceID != nil {
				incoming[*r.TargetInstanceID]++
			}
		}
		total := len(inflight)
		now, err := readStorageClock(ctx, tx, tx.Name())
		if err != nil {
			return err
		}
		var changed []handoffRow
		var changedSlots []ownershipRow
		changedInstances := make(map[string]bool)
		seen := make(map[uint32]bool)
		for _, c := range request.Commands {
			h := c.Handoff
			if seen[h.SlotID] {
				return errors.New("sequence: duplicate handoff command")
			}
			seen[h.SlotID] = true
			o, found := owned[h.SlotID]
			if !found {
				continue
			}
			r, exists := current[h.SlotID]
			if c.Action == "WITHDRAW" {
				if request.InstanceID != h.SourceInstanceID || o.OwnerInstanceID == nil ||
					*o.OwnerInstanceID != h.SourceInstanceID ||
					o.Epoch != h.SourceEpoch ||
					o.State != string(biz.SlotOwned) ||
					(exists && r.handoff().Active()) {
					continue
				}
				h.Kind, h.Phase, h.TargetInstanceID = "RELEASE", "READY", ""
				r = rowForHandoff(h, now)
			} else if !exists || r.ID != h.ID || r.SourceInstanceID != h.SourceInstanceID || r.SourceEpoch != h.SourceEpoch || r.handoff().TargetInstanceID != h.TargetInstanceID {
				continue
			}
			currentHandoff := r.handoff()
			sourceMatches := o.OwnerInstanceID != nil && *o.OwnerInstanceID == r.SourceInstanceID &&
				o.Epoch == r.SourceEpoch
			target := currentHandoff.TargetInstanceID
			targetFresh := freshInstance(leases[target], now, request.HA.LeaseDuration)
			switch c.Action {
			case "PREPARE":
				if (r.Phase != "PLANNED" && r.Phase != "DRAINING") ||
					request.InstanceID != target ||
					!targetFresh ||
					leases[target].Revision != request.Revision ||
					!sourceMatches {
					continue
				}
				r.TargetReady = true
				if r.Phase == "PLANNED" {
					r.Phase = "READY"
				}
			case "BEGIN", "WITHDRAW":
				if r.Phase != "READY" || !sourceMatches || o.State != string(biz.SlotOwned) ||
					(request.InstanceID != r.SourceInstanceID && request.InstanceID != target) {
					continue
				}
				if total >= request.Limits.MaxInflight ||
					outgoing[r.SourceInstanceID] >= request.Limits.MaxPerSource ||
					(r.Kind == "TRANSFER" && (!r.TargetReady || !targetFresh || incoming[target] >= request.Limits.MaxPerTarget)) {
					continue
				}
				source, ok := leases[r.SourceInstanceID]
				if !ok {
					return biz.ErrAuthorityChanged
				}
				boundary := now
				if source.GrantedAt.After(boundary) {
					boundary = source.GrantedAt
				}
				boundary = boundary.Add(request.HA.QuietWindow)
				r.NotBefore, r.Phase = &boundary, "DRAINING"
				o.State = string(biz.SlotDraining)
				changedSlots = append(changedSlots, o)
				changedInstances[r.SourceInstanceID] = true
				total++
				outgoing[r.SourceInstanceID]++
				incoming[target]++
			case "ACK_DRAIN":
				if r.Phase != "DRAINING" || !sourceMatches ||
					request.InstanceID != r.SourceInstanceID {
					continue
				}
				r.Drained = true
			case "TRANSFER":
				if r.Phase != "DRAINING" || !sourceMatches || o.State != string(biz.SlotDraining) ||
					r.NotBefore == nil ||
					(!r.Drained && now.Before(*r.NotBefore)) {
					continue
				}
				if r.Kind == "TRANSFER" {
					if request.InstanceID != target || !targetFresh || !r.TargetReady {
						continue
					}
					o.OwnerInstanceID = r.TargetInstanceID
					o.Epoch++
					o.State = string(biz.SlotOwned)
					r.Phase = "TRANSFERRED"
					changedInstances[target] = true
				} else {
					if request.InstanceID != r.SourceInstanceID {
						continue
					}
					o.OwnerInstanceID = nil
					o.State = string(biz.SlotUnowned)
					r.Phase = "COMPLETED"
				}
				changedSlots = append(changedSlots, o)
				changedInstances[r.SourceInstanceID] = true
			case "ACK_ACTIVE":
				if r.Phase != "TRANSFERRED" || request.InstanceID != target || !targetFresh ||
					leases[target].Revision != request.Revision ||
					o.OwnerInstanceID == nil ||
					*o.OwnerInstanceID != target ||
					o.Epoch != r.SourceEpoch+1 ||
					o.State != string(biz.SlotOwned) {
					continue
				}
				r.Phase = "COMPLETED"
			default:
				return errors.New("sequence: unknown handoff action")
			}
			r.UpdatedAt = now
			changed = append(changed, r)
		}
		if len(changedSlots) > 0 {
			for i := range changedSlots {
				changedSlots[i].UpdatedAt = now
			}
			// All tuples were validated under locks; a single upsert preserves independent epochs.
			if err := tx.Table("sequence_slot_ownership").
				Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "slot_id"}}, DoUpdates: clause.AssignmentColumns([]string{"owner_instance_id", "epoch", "state", "updated_at"})}).
				Create(&changedSlots).
				Error; err != nil {
				return err
			}
		}
		if err := writeHandoffs(tx, changed); err != nil {
			return err
		}
		var changedIDs []string
		for id := range changedInstances {
			changedIDs = append(changedIDs, id)
		}
		if len(changedIDs) > 0 {
			return bumpRevisions(ctx, tx, changedIDs)
		}
		return nil
	})
}

func (d *handoffData) RecoverHandoffs(
	ctx context.Context,
	tenure biz.CoordinatorLease,
	nodes []biz.NodeInfo,
	ha biz.HAConfig,
	limit int,
) error {
	pending, err := d.Handoffs(ctx, "", biz.SlotCount)
	if err != nil || len(pending) == 0 {
		return err
	}
	live := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		live[n.InstanceID] = true
	}
	var candidates []biz.Handoff
	for _, h := range pending {
		if !live[h.TargetInstanceID] {
			candidates = append(candidates, h)
			if len(candidates) == limit {
				break
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := GuardCoordinator(ctx, tx, tx.Name(), tenure); err != nil {
			return err
		}
		var ids []string
		var slots []uint32
		for _, h := range candidates {
			ids = append(ids, h.SourceInstanceID, h.TargetInstanceID)
			slots = append(slots, h.SlotID)
		}
		for _, n := range nodes {
			ids = append(ids, n.InstanceID)
		}
		leases, err := lockInstances(ctx, tx, ids, "UPDATE")
		if err != nil {
			return err
		}
		owned, err := loadSlots(ctx, tx, slots, "UPDATE")
		if err != nil {
			return err
		}
		bySlot := make(map[uint32]ownershipRow)
		for _, o := range owned {
			bySlot[o.SlotID] = o
		}
		rows, err := lockedHandoffs(ctx, tx, slots)
		if err != nil {
			return err
		}
		now, err := readStorageClock(ctx, tx, tx.Name())
		if err != nil {
			return err
		}
		var changed []handoffRow
		var released []uint32
		var revised []string
		for _, h := range candidates {
			r, ok := rows[h.SlotID]
			if !ok || r.ID != h.ID {
				continue
			}
			o := bySlot[h.SlotID]
			if r.Phase == "TRANSFERRED" {
				if o.OwnerInstanceID == nil || *o.OwnerInstanceID != h.TargetInstanceID ||
					o.Epoch != h.SourceEpoch+1 {
					r.Phase = "CANCELLED"
					r.UpdatedAt = now
					changed = append(changed, r)
				}
				continue
			}
			switch r.Phase {
			case "PLANNED", "READY":
				r.Phase = "CANCELLED"
			case "DRAINING":
				if o.OwnerInstanceID == nil || *o.OwnerInstanceID != r.SourceInstanceID ||
					o.Epoch != r.SourceEpoch || o.State != string(biz.SlotDraining) {
					r.Phase = "CANCELLED"
					break
				}
				if r.Kind == "RELEASE" {
					if r.NotBefore == nil || now.Before(*r.NotBefore) {
						continue
					}
					if o.OwnerInstanceID == nil || *o.OwnerInstanceID != r.SourceInstanceID ||
						o.Epoch != r.SourceEpoch {
						r.Phase = "CANCELLED"
					} else {
						released = append(released, r.SlotID)
						revised = append(revised, r.SourceInstanceID)
						r.Phase = "COMPLETED"
					}
				} else {
					var target string
					for _, n := range nodes {
						if n.InstanceID != r.SourceInstanceID &&
							freshInstance(leases[n.InstanceID], now, ha.LeaseDuration) {
							target = n.InstanceID
							break
						}
					}
					if target == "" {
						// Releasing preserves the original drain fence and lets the
						// sole surviving source re-claim at a fresh epoch.
						r.Kind = "RELEASE"
						r.TargetInstanceID = nil
						r.ID = uuid.NewString()
						r.TargetReady = false
						break
					}
					r.TargetInstanceID = &target
					r.ID = uuid.NewString()
					r.Drained = false
					r.TargetReady = false
				}
			default:
				continue
			}
			r.UpdatedAt = now
			changed = append(changed, r)
		}
		if len(released) > 0 {
			if err := tx.Table("sequence_slot_ownership").
				Where("slot_id IN ?", released).
				Updates(map[string]any{"owner_instance_id": nil, "state": "UNOWNED", "updated_at": now}).
				Error; err != nil {
				return err
			}
			if err := bumpRevisions(ctx, tx, revised); err != nil {
				return err
			}
		}
		return writeHandoffs(tx, changed)
	})
}

func (d *handoffData) CollectInstances(ctx context.Context, quiet time.Duration, limit int) error {
	age, err := elapsedExpression(d.db.Name(), "i.granted_at")
	if err != nil {
		return err
	}
	var ids []string
	err = d.db.WithContext(ctx).
		Raw("SELECT i.instance_id FROM sequence_instance_leases i WHERE "+age+" >= ? AND NOT EXISTS (SELECT 1 FROM sequence_slot_ownership o WHERE o.owner_instance_id=i.instance_id) AND NOT EXISTS (SELECT 1 FROM sequence_node_liveness n WHERE n.instance_id=i.instance_id) AND NOT EXISTS (SELECT 1 FROM sequence_slot_handoffs h WHERE h.phase NOT IN ('COMPLETED','CANCELLED') AND (h.source_instance_id=i.instance_id OR h.target_instance_id=i.instance_id)) ORDER BY i.instance_id LIMIT ?", quiet.Microseconds(), limit).
		Scan(&ids).
		Error
	if err != nil || len(ids) == 0 {
		return err
	}
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, err := lockInstances(ctx, tx, ids, "UPDATE"); err != nil {
			return err
		}
		age, err := elapsedExpression(tx.Name(), "granted_at")
		if err != nil {
			return err
		}
		return tx.Exec(
			"DELETE FROM sequence_instance_leases WHERE instance_id IN ? AND "+age+" >= ? AND NOT EXISTS (SELECT 1 FROM sequence_slot_ownership o WHERE o.owner_instance_id=sequence_instance_leases.instance_id) AND NOT EXISTS (SELECT 1 FROM sequence_node_liveness n WHERE n.instance_id=sequence_instance_leases.instance_id) AND NOT EXISTS (SELECT 1 FROM sequence_slot_handoffs h WHERE h.phase NOT IN ('COMPLETED','CANCELLED') AND (h.source_instance_id=sequence_instance_leases.instance_id OR h.target_instance_id=sequence_instance_leases.instance_id))",
			ids,
			quiet.Microseconds(),
		).Error
	})
}
