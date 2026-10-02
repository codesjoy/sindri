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
	"fmt"
	"sort"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ownershipRow struct {
	SlotID          uint32    `gorm:"column:slot_id"`
	OwnerInstanceID *string   `gorm:"column:owner_instance_id"`
	Epoch           uint64    `gorm:"column:epoch"`
	State           string    `gorm:"column:state"`
	UpdatedAt       time.Time `gorm:"column:updated_at"`
}

const ownershipColumns = "slot_id, owner_instance_id, epoch, state"

type instanceRow struct {
	InstanceID string    `gorm:"column:instance_id;primaryKey"`
	NodeID     string    `gorm:"column:node_id"`
	Revision   uint64    `gorm:"column:ownership_revision"`
	GrantedAt  time.Time `gorm:"column:granted_at"`
	State      string    `gorm:"column:state"`
}

func (r instanceRow) lease() biz.InstanceLease {
	return biz.InstanceLease{
		InstanceID: r.InstanceID,
		NodeID:     r.NodeID,
		Revision:   r.Revision,
		GrantedAt:  r.GrantedAt,
		State:      r.State,
	}
}

func (r ownershipRow) ownership(leases map[string]instanceRow) biz.Ownership {
	o := biz.Ownership{SlotID: r.SlotID, Epoch: r.Epoch, State: biz.SlotState(r.State)}
	if r.OwnerInstanceID != nil {
		o.OwnerInstanceID = *r.OwnerInstanceID
		o.OwnerNodeID = leases[o.OwnerInstanceID].NodeID
	}
	return o
}

type ownershipData struct{ db *gorm.DB }

// NewOwnershipData builds the durable instance and slot authority store over db.
func NewOwnershipData(db *gorm.DB) biz.OwnershipRepo { return &ownershipData{db: db} }

func readStorageClock(ctx context.Context, tx *gorm.DB, dialect string) (time.Time, error) {
	if dialect == "sqlite" {
		var raw struct{ Now string }
		if err := tx.WithContext(ctx).
			Raw("SELECT strftime('%Y-%m-%d %H:%M:%f', 'now') AS now").
			Scan(&raw).
			Error; err != nil {
			return time.Time{}, err
		}
		return time.Parse("2006-01-02 15:04:05.000", raw.Now)
	}
	expression, err := storageNowExpression(dialect)
	if err != nil {
		return time.Time{}, err
	}
	var clock struct{ Now time.Time }
	if err := tx.WithContext(ctx).
		Raw("SELECT " + expression + " AS now").
		Scan(&clock).
		Error; err != nil {
		return time.Time{}, err
	}
	return clock.Now, nil
}

func (d *ownershipData) StorageClock(ctx context.Context) (time.Time, error) {
	return readStorageClock(ctx, d.db, d.db.Name())
}

// Every authority mutation locks instances before slots, even if discovery read an old owner.
func lockInstances(
	ctx context.Context,
	tx *gorm.DB,
	ids []string,
	strength string,
) (map[string]instanceRow, error) {
	unique := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			unique[id] = struct{}{}
		}
	}
	ids = make([]string, 0, len(unique))
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make(map[string]instanceRow, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	query := tx.WithContext(ctx).
		Table("sequence_instance_leases").
		Where("instance_id IN ?", ids).
		Order("instance_id")
	if tx.Name() != "sqlite" {
		query = query.Clauses(clause.Locking{Strength: strength})
	}
	var rows []instanceRow
	if err := query.Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("lock instance authority: %w", err)
	}
	for _, row := range rows {
		result[row.InstanceID] = row
	}
	return result, nil
}

func loadSlots(
	ctx context.Context,
	tx *gorm.DB,
	slots []uint32,
	strength string,
) ([]ownershipRow, error) {
	query := tx.WithContext(ctx).
		Table("sequence_slot_ownership").
		Select(ownershipColumns).
		Where("slot_id IN ?", slots).
		Order("slot_id")
	if strength != "" && tx.Name() != "sqlite" {
		query = query.Clauses(clause.Locking{Strength: strength})
	}
	var rows []ownershipRow
	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func bumpRevisions(ctx context.Context, tx *gorm.DB, ids []string) error {
	now, err := storageNowExpression(tx.Name())
	if err != nil {
		return err
	}
	return tx.WithContext(ctx).Table("sequence_instance_leases").Where("instance_id IN ?", ids).
		Updates(map[string]any{"ownership_revision": gorm.Expr("ownership_revision + 1"), "updated_at": gorm.Expr(now)}).
		Error
}

func (d *ownershipData) RegisterInstance(ctx context.Context, nodeID, instanceID string) error {
	if nodeID == "" || instanceID == "" {
		return errors.New("sequence: instance identity is required")
	}
	now, err := storageNowExpression(d.db.Name())
	if err != nil {
		return err
	}
	row := map[string]any{
		"instance_id":        instanceID,
		"node_id":            nodeID,
		"ownership_revision": 0,
		"state":              "ACTIVE",
		"granted_at":         gorm.Expr(now),
		"created_at":         gorm.Expr(now),
		"updated_at":         gorm.Expr(now),
	}
	onConflict := clause.OnConflict{DoNothing: true}
	if d.db.Name() == "mysql" {
		// GORM renders DoNothing for MySQL as an empty ON DUPLICATE KEY UPDATE
		// clause. Assigning the primary key to itself preserves the intended
		// insert-if-absent behavior without hiding other insert failures.
		onConflict = clause.OnConflict{DoUpdates: clause.Assignments(map[string]any{
			"instance_id": gorm.Expr("instance_id"),
		})}
	}
	if err := d.db.WithContext(ctx).
		Table("sequence_instance_leases").
		Clauses(onConflict).
		Create(row).
		Error; err != nil {
		return err
	}
	var existing instanceRow
	if err := d.db.WithContext(ctx).
		Table("sequence_instance_leases").
		Where("instance_id = ?", instanceID).
		Take(&existing).
		Error; err != nil {
		return err
	}
	if existing.NodeID != nodeID || existing.State != "ACTIVE" {
		return biz.ErrAuthorityChanged
	}
	return nil
}

func (d *ownershipData) RenewInstance(
	ctx context.Context,
	instanceID string,
	revision uint64,
) (biz.InstanceLease, error) {
	var renewed instanceRow
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		leases, err := lockInstances(ctx, tx, []string{instanceID}, "UPDATE")
		if err != nil {
			return err
		}
		row, ok := leases[instanceID]
		if !ok || row.State != "ACTIVE" || row.Revision != revision {
			return biz.ErrAuthorityChanged
		}
		now, err := readStorageClock(ctx, tx, tx.Name())
		if err != nil {
			return err
		}
		if err := tx.Table("sequence_instance_leases").Where("instance_id = ?", instanceID).
			Updates(map[string]any{"granted_at": now, "updated_at": now}).Error; err != nil {
			return err
		}
		row.GrantedAt = now
		renewed = row
		return nil
	})
	if err != nil {
		return biz.InstanceLease{}, classifyReservationError(err)
	}
	return renewed.lease(), nil
}

func (d *ownershipData) InstanceAuthority(
	ctx context.Context,
	instanceID string,
) (biz.AuthoritySnapshot, error) {
	var snapshot biz.AuthoritySnapshot
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		leases, err := lockInstances(ctx, tx, []string{instanceID}, "SHARE")
		if err != nil {
			return err
		}
		lease, ok := leases[instanceID]
		if !ok || lease.State != "ACTIVE" {
			return biz.ErrAuthorityChanged
		}
		snapshot.Lease = lease.lease()
		var rows []ownershipRow
		if err := tx.Table("sequence_slot_ownership").
			Select(ownershipColumns).
			Where("owner_instance_id = ?", instanceID).
			Order("slot_id").
			Limit(biz.SlotCount).
			Scan(&rows).
			Error; err != nil {
			return err
		}
		for _, r := range rows {
			snapshot.Slots = append(snapshot.Slots, r.ownership(leases))
		}
		return nil
	})
	return snapshot, err
}

func (d *ownershipData) RetireInstance(ctx context.Context, instanceID string) error {
	now, err := storageNowExpression(d.db.Name())
	if err != nil {
		return err
	}
	return d.db.WithContext(ctx).
		Table("sequence_instance_leases").
		Where("instance_id = ? AND state = 'ACTIVE'", instanceID).
		Updates(map[string]any{"state": "RETIRED", "ownership_revision": gorm.Expr("ownership_revision + 1"), "updated_at": gorm.Expr(now)}).
		Error
}

func (d *ownershipData) ClaimSlots(
	ctx context.Context,
	request biz.ClaimRequest,
) ([]biz.ClaimOutcome, error) {
	if request.InstanceID == "" || request.Lease <= 0 || request.QuietWindow <= 0 ||
		len(request.Slots) > 1000 {
		return nil, errors.New("sequence: invalid claim")
	}
	if len(request.Slots) == 0 {
		return nil, nil
	}
	discovered, err := loadSlots(ctx, d.db, request.Slots, "")
	if err != nil {
		return nil, err
	}
	ids := []string{request.InstanceID}
	for _, r := range discovered {
		if r.OwnerInstanceID != nil {
			ids = append(ids, *r.OwnerInstanceID)
		}
	}
	var outcomes []biz.ClaimOutcome
	err = d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		leases, err := lockInstances(ctx, tx, ids, "UPDATE")
		if err != nil {
			return err
		}
		rows, err := loadSlots(ctx, tx, request.Slots, "UPDATE")
		if err != nil {
			return err
		}
		if len(rows) != len(request.Slots) {
			return biz.ErrSlotNotOwned
		}
		intents, err := lockedHandoffs(ctx, tx, request.Slots)
		if err != nil {
			return err
		}
		now, err := readStorageClock(ctx, tx, tx.Name())
		if err != nil {
			return err
		}
		candidate, ok := leases[request.InstanceID]
		if !ok || candidate.State != "ACTIVE" || candidate.NodeID != request.NodeID ||
			candidate.Revision != request.Revision {
			return biz.ErrAuthorityChanged
		}
		if !now.Before(candidate.GrantedAt.Add(request.Lease)) {
			return biz.ErrLeaseExpired
		}
		var granted []uint32
		changed := map[string]bool{}
		for _, row := range rows {
			o := biz.ClaimOutcome{Ownership: row.ownership(leases)}
			if row.OwnerInstanceID != nil {
				owner, found := leases[*row.OwnerInstanceID]
				if !found {
					return biz.ErrAuthorityChanged
				}
				o.NotBefore = owner.GrantedAt.Add(request.QuietWindow)
			}
			self := row.State == string(biz.SlotOwned) && row.OwnerInstanceID != nil &&
				*row.OwnerInstanceID == request.InstanceID
			eligible := row.State == string(biz.SlotUnowned) ||
				(row.State == string(biz.SlotOwned) && !now.Before(o.NotBefore))
			if row.State == string(biz.SlotDraining) {
				h, found := intents[row.SlotID]
				eligible = found && h.Kind == "RELEASE" && h.Phase == "DRAINING" &&
					h.SourceEpoch == row.Epoch &&
					row.OwnerInstanceID != nil &&
					h.SourceInstanceID == *row.OwnerInstanceID &&
					h.NotBefore != nil &&
					(h.Drained || !now.Before(*h.NotBefore))
			}
			if self {
				o.Granted = true
			} else if eligible {
				granted = append(granted, row.SlotID)
				changed[request.InstanceID] = true
				if row.OwnerInstanceID != nil {
					changed[*row.OwnerInstanceID] = true
				}
				o.Ownership = biz.Ownership{
					SlotID:          row.SlotID,
					OwnerNodeID:     request.NodeID,
					OwnerInstanceID: request.InstanceID,
					Epoch:           row.Epoch + 1,
					State:           biz.SlotOwned,
				}
				o.Granted = true
			}
			if o.Granted {
				o.NotBefore = time.Time{}
			}
			outcomes = append(outcomes, o)
		}
		if len(granted) == 0 {
			return nil
		}
		if err := tx.Table("sequence_slot_ownership").Where("slot_id IN ?", granted).
			Updates(map[string]any{"owner_instance_id": request.InstanceID, "epoch": gorm.Expr("epoch + 1"), "state": string(biz.SlotOwned), "updated_at": now}).
			Error; err != nil {
			return err
		}
		if err := tx.Table("sequence_slot_handoffs").
			Where("slot_id IN ? AND phase NOT IN ('COMPLETED', 'CANCELLED')", granted).
			Updates(map[string]any{"phase": "CANCELLED", "updated_at": now}).
			Error; err != nil {
			return err
		}
		changedIDs := make([]string, 0, len(changed))
		for id := range changed {
			changedIDs = append(changedIDs, id)
		}
		return bumpRevisions(ctx, tx, changedIDs)
	})
	if err != nil {
		return nil, classifyReservationError(err)
	}
	return outcomes, nil
}

func lockClause(_ *gorm.DB, strength string) clause.Expression {
	// SQLite's dialect builder omits FOR clauses; an empty Expr would instead
	// be appended to WHERE and produce a malformed predicate.
	return clause.Locking{Strength: strength}
}
