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

// Package data implements the sequence repositories over GORM: range
// reservation, ownership, placement, liveness, and route persistence.
package data

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	gorm "gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// defaultNamespace is the namespace every key uses today. Section 5.2 models
// the high watermark as (namespace, key); the column exists so that introducing
// a real namespace later is a data migration rather than a schema change. A
// non-empty namespace also changes slot placement, so it must ship with a
// layout_version bump and a client release.
const defaultNamespace = ""

// SequenceModel stores the reserved high watermark of one key.
type SequenceModel struct {
	Namespace   string    `gorm:"column:namespace;size:64;primaryKey"`
	SequenceKey string    `gorm:"column:sequence_key;size:256;primaryKey"`
	ReservedEnd int64     `gorm:"column:reserved_end;not null"`
	UpdatedAt   time.Time `gorm:"column:updated_at;not null"`
}

// TableName returns the sequence range table name.
func (SequenceModel) TableName() string { return "sequence_ranges" }

type sequenceData struct {
	db *gorm.DB
}

// NewSequenceData constructs the sequence repository backed by db.
func NewSequenceData(db *gorm.DB) biz.SequenceRepo {
	return &sequenceData{db: db}
}

// reservationTarget is one key placed in its ownership slot.
type reservationTarget struct {
	key    string
	slotID uint32
	step   int64
}

func (d *sequenceData) ReserveRanges(
	ctx context.Context,
	authority biz.ReservationAuthority,
	requests []biz.ReservationRequest,
) ([]biz.SequenceRange, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("sequence gorm store: database is required")
	}
	if authority.InstanceID == "" {
		return nil, errors.New("sequence gorm store: reservation authority is required")
	}
	if err := validateReservationRequests(requests); err != nil {
		return nil, err
	}

	targets := make([]reservationTarget, len(requests))
	for index, request := range requests {
		targets[index] = reservationTarget{
			key:    request.Key,
			slotID: biz.SlotForKey(request.Key),
			step:   request.Step,
		}
	}
	// Stable order is by (slot, key), not by key alone: keys sorted lexically do
	// not imply slot order, so a key-only order could deadlock two batches that
	// interleave on the ownership rows.
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].slotID != targets[j].slotID {
			return targets[i].slotID < targets[j].slotID
		}
		return targets[i].key < targets[j].key
	})

	slots := make([]uint32, 0, len(targets))
	for _, target := range targets {
		if len(slots) == 0 || slots[len(slots)-1] != target.slotID {
			slots = append(slots, target.slotID)
		}
	}

	var (
		reserved []biz.SequenceRange
		err      error
	)
	switch d.db.Name() {
	case "postgres":
		reserved, err = d.reservePostgres(ctx, authority, requests, targets, slots)
	case "mysql", "sqlite":
		reserved, err = d.reserveTransactional(ctx, authority, requests, targets, slots)
	default:
		return nil, fmt.Errorf(
			"sequence gorm store: unsupported dialect %q",
			d.db.Name(),
		)
	}
	if err != nil {
		return nil, err
	}
	return reserved, nil
}

func validateReservationRequests(requests []biz.ReservationRequest) error {
	if len(requests) == 0 {
		return errors.New("sequence gorm store: at least one reservation is required")
	}
	seen := make(map[string]struct{}, len(requests))
	for _, request := range requests {
		if strings.TrimSpace(request.Key) == "" || len(request.Key) > 256 {
			return errors.New("sequence gorm store: key must contain 1..256 bytes")
		}
		if request.Step <= 0 {
			return errors.New("sequence gorm store: step must be positive")
		}
		if _, exists := seen[request.Key]; exists {
			return fmt.Errorf("sequence gorm store: duplicate key %q", request.Key)
		}
		seen[request.Key] = struct{}{}
	}
	return nil
}

// lockOwnership reads the authority rows in a way that blocks a concurrent
// takeover for the duration of the transaction. A plain snapshot read would not
// be a fence: the interleaving in section 2.1 lets a takeover commit between the
// check and the watermark write, so both sides succeed and the reservation runs
// under a superseded epoch.
func lockOwnership(
	ctx context.Context,
	tx *gorm.DB,
	dialect string,
	slots []uint32,
) ([]ownershipRow, error) {
	query := tx.WithContext(ctx).Table("slot_ownership").
		Select(ownershipColumns).
		Where("slot_id IN ?", slots).
		Order("slot_id")
	if dialect != "sqlite" {
		query = query.Clauses(clause.Locking{Strength: "SHARE"})
	}
	var rows []ownershipRow
	if err := query.Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("lock slot ownership: %w", err)
	}
	return rows, nil
}

// checkAuthority validates the locked authority rows before any watermark moves.
func checkAuthority(
	authority biz.ReservationAuthority,
	slots []uint32,
	locked []ownershipRow,
	now time.Time,
) error {
	bySlot := make(map[uint32]ownershipRow, len(locked))
	for _, row := range locked {
		bySlot[row.SlotID] = row
	}
	for _, slotID := range slots {
		row, ok := bySlot[slotID]
		if !ok {
			return fmt.Errorf("%w: slot %d has no ownership row", biz.ErrSlotNotOwned, slotID)
		}
		if row.State != string(biz.SlotOwned) || row.OwnerInstanceID == nil ||
			*row.OwnerInstanceID != authority.InstanceID {
			return fmt.Errorf("%w: slot %d", biz.ErrSlotNotOwned, slotID)
		}
		if authority.Epochs != nil {
			epoch, ok := authority.Epochs[slotID]
			if !ok || row.Epoch != epoch {
				return fmt.Errorf(
					"%w: slot %d epoch %d does not match %d",
					biz.ErrSlotNotOwned,
					slotID,
					row.Epoch,
					epoch,
				)
			}
		}
		if authority.Lease > 0 {
			if row.GrantedAt == nil || now.After(row.GrantedAt.Add(authority.Lease)) {
				return fmt.Errorf("%w: slot %d", biz.ErrLeaseExpired, slotID)
			}
		}
	}
	return nil
}

func (d *sequenceData) reservePostgres(
	ctx context.Context,
	authority biz.ReservationAuthority,
	requests []biz.ReservationRequest,
	targets []reservationTarget,
	slots []uint32,
) ([]biz.SequenceRange, error) {
	var reserved []biz.SequenceRange
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		locked, err := lockOwnership(ctx, tx, "postgres", slots)
		if err != nil {
			return err
		}
		now, err := readStorageClock(ctx, tx, d.db.Name())
		if err != nil {
			return err
		}
		if err := checkAuthority(authority, slots, locked, now); err != nil {
			return err
		}

		query, args := batchInsertQuery(targets, "clock_timestamp()",
			" ON CONFLICT (namespace, sequence_key) DO UPDATE SET "+
				"reserved_end = sequence_ranges.reserved_end + EXCLUDED.reserved_end, "+
				"updated_at = clock_timestamp() RETURNING sequence_key, reserved_end")
		var rows []SequenceModel
		if err := tx.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
			return fmt.Errorf("reserve sequence ranges: %w", err)
		}
		reserved, err = buildReservedRanges(requests, rows)
		return err
	})
	if err != nil {
		return nil, classifyReservationError(err)
	}
	return reserved, nil
}

func (d *sequenceData) reserveTransactional(
	ctx context.Context,
	authority biz.ReservationAuthority,
	requests []biz.ReservationRequest,
	targets []reservationTarget,
	slots []uint32,
) ([]biz.SequenceRange, error) {
	dialect := d.db.Name()
	var reserved []biz.SequenceRange
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		locked, err := lockOwnership(ctx, tx, dialect, slots)
		if err != nil {
			return err
		}
		now, err := readStorageClock(ctx, tx, dialect)
		if err != nil {
			return err
		}
		if err := checkAuthority(authority, slots, locked, now); err != nil {
			return err
		}

		suffix := " ON DUPLICATE KEY UPDATE " +
			"reserved_end = sequence_ranges.reserved_end + VALUES(reserved_end), " +
			"updated_at = VALUES(updated_at)"
		if dialect == "sqlite" {
			suffix = " ON CONFLICT (namespace, sequence_key) DO UPDATE SET " +
				"reserved_end = sequence_ranges.reserved_end + excluded.reserved_end, " +
				"updated_at = excluded.updated_at"
		}
		timestamp, err := StorageNowExpression(dialect)
		if err != nil {
			return err
		}
		query, args := batchInsertQuery(targets, timestamp, suffix)
		if err := tx.Exec(query, args...).Error; err != nil {
			return fmt.Errorf("advance sequence ranges: %w", err)
		}

		keys := make([]string, len(targets))
		for index, target := range targets {
			keys[index] = target.key
		}
		var rows []SequenceModel
		dbQuery := tx.WithContext(ctx).
			Where("namespace = ? AND sequence_key IN ?", defaultNamespace, keys)
		if dialect != "sqlite" {
			dbQuery = dbQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := dbQuery.Find(&rows).Error; err != nil {
			return fmt.Errorf("read reserved sequence ranges: %w", err)
		}
		reserved, err = buildReservedRanges(requests, rows)
		return err
	})
	if err != nil {
		return nil, classifyReservationError(err)
	}
	return reserved, nil
}

// batchInsertQuery builds one multi-row upsert. The timestamp is supplied by the
// database expression rather than bound from the node clock, so updated_at and
// granted_at share one clock domain.
func batchInsertQuery(
	targets []reservationTarget,
	timestampExpression string,
	suffix string,
) (string, []any) {
	var query strings.Builder
	query.WriteString(
		"INSERT INTO sequence_ranges (namespace, sequence_key, reserved_end, updated_at) VALUES ",
	)
	args := make([]any, 0, len(targets)*3)
	for index, target := range targets {
		if index > 0 {
			query.WriteString(", ")
		}
		query.WriteString("(?, ?, ?, " + timestampExpression + ")")
		args = append(args, defaultNamespace, target.key, target.step)
	}
	query.WriteString(suffix)
	return query.String(), args
}

func buildReservedRanges(
	requests []biz.ReservationRequest,
	rows []SequenceModel,
) ([]biz.SequenceRange, error) {
	byKey := make(map[string]SequenceModel, len(rows))
	for _, row := range rows {
		byKey[row.SequenceKey] = row
	}

	reserved := make([]biz.SequenceRange, len(requests))
	for index, request := range requests {
		row, ok := byKey[request.Key]
		if !ok {
			return nil, fmt.Errorf(
				"reserve sequence range %q: database did not return a value",
				request.Key,
			)
		}
		if row.ReservedEnd < request.Step {
			return nil, fmt.Errorf(
				"reserve sequence %q: invalid maximum %d",
				request.Key,
				row.ReservedEnd,
			)
		}
		reserved[index] = biz.SequenceRange{
			Start: row.ReservedEnd - request.Step + 1,
			End:   row.ReservedEnd,
		}
		if reserved[index].Start <= 0 {
			return nil, fmt.Errorf("reserve sequence %q: maximum overflow", request.Key)
		}
	}
	return reserved, nil
}

// classifyReservationError distinguishes "this did not commit" from "this may
// have committed". The distinction matters because the caller must discard an
// uncertain range and must never infer it by re-reading the watermark
// (section 6.1). Discarding a range that did commit only leaves a gap, which is
// allowed; inferring one that did not would hand out overlapping IDs.
func classifyReservationError(err error) error {
	if err == nil || !isUncertainCommit(err) {
		return err
	}
	return fmt.Errorf("%w: %w", biz.ErrCommitUncertain, err)
}

func isUncertainCommit(err error) bool {
	if errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// The MySQL driver reports a dropped connection as a plain error string
	// rather than a typed one.
	message := err.Error()
	return strings.Contains(message, "invalid connection") ||
		strings.Contains(message, "server has gone away") ||
		strings.Contains(message, "connection refused")
}
