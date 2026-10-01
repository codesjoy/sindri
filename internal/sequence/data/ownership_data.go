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
	"strings"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ownershipRow is the column subset the authority decisions need.
type ownershipRow struct {
	SlotID          uint32     `gorm:"column:slot_id"`
	OwnerNodeID     *string    `gorm:"column:owner_node_id"`
	OwnerInstanceID *string    `gorm:"column:owner_instance_id"`
	Epoch           uint64     `gorm:"column:epoch"`
	GrantedAt       *time.Time `gorm:"column:granted_at"`
	State           string     `gorm:"column:state"`
	// GrantedAgoMicros is populated by the readers that add the age expression to
	// their select list -- LoadOwnership and the placement view. It is nil
	// whenever granted_at is, which is every unowned row.
	GrantedAgoMicros *int64 `gorm:"column:granted_ago_micros"`
}

const ownershipColumns = "slot_id, owner_node_id, owner_instance_id, epoch, granted_at, state"

// ownershipColumnsQualified is the same list qualified for statements that join
// another relation carrying the same column names, where an unqualified
// reference would be ambiguous.
const ownershipColumnsQualified = "o.slot_id, o.owner_node_id, o.owner_instance_id, " +
	"o.epoch, o.granted_at, o.state"

// storageClock is the single-column result of reading the storage lease clock.
// It is scanned through a struct because GORM's Scan only reliably maps
// destinations that are structs or slices of them.
type storageClock struct {
	Now time.Time `gorm:"column:now"`
}

// sqliteClockLayout matches strftime('%Y-%m-%d %H:%M:%f').
const sqliteClockLayout = "2006-01-02 15:04:05.000"

// readStorageClock returns the storage lease time observed by the database.
//
// SQLite needs its own branch: a computed CURRENT_TIMESTAMP has no declared
// column type, so the driver hands it back as a string rather than converting it
// to a time. Writing the plain expression is still correct there, because the
// target columns are declared datetime and the driver converts them on read.
func readStorageClock(ctx context.Context, tx *gorm.DB, dialect string) (time.Time, error) {
	if dialect == "sqlite" {
		var raw struct {
			Now string `gorm:"column:now"`
		}
		if err := tx.WithContext(ctx).
			Raw("SELECT strftime('%Y-%m-%d %H:%M:%f', 'now') AS now").
			Scan(&raw).Error; err != nil {
			return time.Time{}, fmt.Errorf("read storage lease time: %w", err)
		}
		parsed, err := time.Parse(sqliteClockLayout, raw.Now)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse storage lease time %q: %w", raw.Now, err)
		}
		return parsed.UTC(), nil
	}
	expression, err := StorageNowExpression(dialect)
	if err != nil {
		return time.Time{}, err
	}
	var clock storageClock
	if err := tx.WithContext(ctx).
		Raw("SELECT " + expression + " AS now").
		Scan(&clock).Error; err != nil {
		return time.Time{}, fmt.Errorf("read storage lease time: %w", err)
	}
	return clock.Now, nil
}

type ownershipData struct {
	db *gorm.DB
}

// NewOwnershipData constructs the slot ownership authority backed by db.
func NewOwnershipData(db *gorm.DB) biz.OwnershipRepo {
	return &ownershipData{db: db}
}

func (d *ownershipData) LoadOwnership(
	ctx context.Context,
	slots []uint32,
) ([]biz.Ownership, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("sequence ownership: database is required")
	}
	if len(slots) == 0 {
		return nil, nil
	}
	age, err := OwnershipAgeExpression(d.db.Name())
	if err != nil {
		return nil, err
	}
	var rows []ownershipRow
	err = d.db.WithContext(ctx).
		Table("slot_ownership").
		Select(ownershipColumns+", "+age+" AS granted_ago_micros").
		Where("slot_id IN ?", slots).
		Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("load slot ownership: %w", err)
	}
	ownership := make([]biz.Ownership, 0, len(rows))
	for _, row := range rows {
		ownership = append(ownership, row.ownership())
	}
	return ownership, nil
}

// StorageClock reads the storage lease clock on its own connection, which is
// what the clock monitor compares against the local clock.
func (d *ownershipData) StorageClock(ctx context.Context) (time.Time, error) {
	if d == nil || d.db == nil {
		return time.Time{}, errors.New("sequence ownership: database is required")
	}
	return readStorageClock(ctx, d.db.WithContext(ctx), d.db.Name())
}

func (r ownershipRow) ownership() biz.Ownership {
	ownership := biz.Ownership{
		SlotID: r.SlotID,
		Epoch:  r.Epoch,
		State:  biz.SlotState(r.State),
	}
	if r.OwnerNodeID != nil {
		ownership.OwnerNodeID = *r.OwnerNodeID
	}
	if r.OwnerInstanceID != nil {
		ownership.OwnerInstanceID = *r.OwnerInstanceID
	}
	if r.GrantedAgoMicros != nil {
		ownership.GrantedAgo = time.Duration(*r.GrantedAgoMicros) * time.Microsecond
		ownership.GrantAgeKnown = true
	}
	return ownership
}

// statementChunkSize bounds how many rows one authority statement carries. A node
// claims, renews and releases every slot assigned to it at once, which can be all
// of them, and both dialects cap how many bind parameters a statement may carry --
// so a whole-space operation has to be split, with each statement under the cap.
const statementChunkSize = 1000

// ClaimSlots grants this instance the authority for the slots it asks for and
// returns one outcome per requested slot.
//
// A slot is granted when it is unowned, when the caller already holds it, or
// when the holder's grant has aged past the quiet window. The second case is
// what makes a claim idempotent for its own claimant, and the node depends on
// it: the authority for a route is claimed in batches of ownershipBatchSize, a
// batch that cannot grant every slot it was given makes the caller drop the
// whole claim and retry it later, and rows granted by the earlier attempt are
// then inside the very window that attempt's successor is checking. Without the
// self-owned case each retry refuses what the previous one took, no attempt can
// ever grant an entire multi-batch claim, and a route that has to be claimed in
// more than one batch never loads at all.
func (d *ownershipData) ClaimSlots(
	ctx context.Context,
	request biz.ClaimRequest,
) ([]biz.ClaimOutcome, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("sequence ownership: database is required")
	}
	if len(request.Slots) == 0 {
		return nil, nil
	}
	if request.InstanceID == "" {
		return nil, errors.New("sequence ownership: instance id is required to claim slots")
	}
	if request.QuietWindow < 0 {
		return nil, errors.New("sequence ownership: quiet window must not be negative")
	}
	switch d.db.Name() {
	case "postgres":
		return d.claimPostgres(ctx, request)
	case "mysql", "sqlite":
		return d.claimTransactional(ctx, request)
	default:
		return nil, fmt.Errorf(
			"sequence ownership: unsupported dialect %q",
			d.db.Name(),
		)
	}
}

func (d *ownershipData) claimPostgres(
	ctx context.Context,
	request biz.ClaimRequest,
) ([]biz.ClaimOutcome, error) {
	var outcomes []biz.ClaimOutcome
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// A single clock reading pins the quiet-window comparison: PostgreSQL
		// evaluates volatile functions per row, so reading clock_timestamp()
		// twice could straddle the window boundary within one statement.
		//
		// The predicate grants a row the claimant already holds. That is not a
		// takeover -- there is no other owner to fence -- and without it a claim
		// that had to be split over statements could never commit: the rows its
		// first statement granted would be inside their own quiet window when the
		// caller retried the whole claim, so every retry would refuse what the
		// attempt before it took and the route would never load (see the claim
		// comment on ClaimSlots).
		query := "WITH lease AS (SELECT clock_timestamp() AS now) " +
			"UPDATE slot_ownership SET " +
			"epoch = slot_ownership.epoch + 1, " +
			"owner_node_id = ?, owner_instance_id = ?, " +
			"granted_at = lease.now, state = 'OWNED', updated_at = lease.now " +
			"FROM lease WHERE slot_id IN ? " +
			"AND (state = 'UNOWNED' OR owner_instance_id = ? " +
			"OR granted_at <= lease.now - make_interval(secs => ?)) " +
			"RETURNING " + ownershipColumns
		var granted []ownershipRow
		if err := tx.Raw(query, request.NodeID, request.InstanceID, request.Slots,
			request.InstanceID, request.QuietWindow.Seconds()).Scan(&granted).Error; err != nil {
			return fmt.Errorf("claim slot ownership: %w", err)
		}
		var readErr error
		outcomes, readErr = d.readClaimOutcomes(ctx, tx, request, granted)
		return readErr
	})
	if err != nil {
		return nil, err
	}
	return outcomes, nil
}

func (d *ownershipData) claimTransactional(
	ctx context.Context,
	request biz.ClaimRequest,
) ([]biz.ClaimOutcome, error) {
	var outcomes []biz.ClaimOutcome
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Locking the rows before deciding is what makes the quiet-window test
		// and the epoch increment one serialisable decision (section 6.4).
		var locked []ownershipRow
		lockQuery := tx.WithContext(ctx).Table("slot_ownership").
			Select(ownershipColumns).
			Where("slot_id IN ?", request.Slots).
			Order("slot_id")
		if d.db.Name() != "sqlite" {
			lockQuery = lockQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := lockQuery.Scan(&locked).Error; err != nil {
			return fmt.Errorf("lock slot ownership: %w", err)
		}
		now, err := readStorageClock(ctx, tx, d.db.Name())
		if err != nil {
			return err
		}

		grantable := make([]uint32, 0, len(locked))
		for _, row := range locked {
			// A row the claimant already holds is granted without waiting for the
			// quiet window: that window protects against a previous owner which
			// may still be allocating, and here the owner is the claimant itself.
			// Refusing it instead would make a claim that has to be split over
			// statements impossible to complete -- see the claim comment on
			// ClaimSlots.
			alreadyHeld := row.OwnerInstanceID != nil &&
				*row.OwnerInstanceID == request.InstanceID
			leaseElapsed := row.GrantedAt != nil &&
				!row.GrantedAt.After(now.Add(-request.QuietWindow))
			if row.State == string(biz.SlotUnowned) || alreadyHeld || leaseElapsed {
				grantable = append(grantable, row.SlotID)
			}
		}
		var granted []ownershipRow
		if len(grantable) > 0 {
			nowExpression, expressionErr := StorageNowExpression(d.db.Name())
			if expressionErr != nil {
				return expressionErr
			}
			update := tx.WithContext(ctx).Table("slot_ownership").
				Where("slot_id IN ?", grantable).
				Updates(map[string]any{
					"epoch":             gorm.Expr("epoch + 1"),
					"owner_node_id":     request.NodeID,
					"owner_instance_id": request.InstanceID,
					"granted_at":        gorm.Expr(nowExpression),
					"state":             string(biz.SlotOwned),
					"updated_at":        gorm.Expr(nowExpression),
				})
			if err := update.Error; err != nil {
				return fmt.Errorf("claim slot ownership: %w", err)
			}
			if err := tx.WithContext(ctx).Table("slot_ownership").
				Select(ownershipColumns).
				Where("slot_id IN ?", grantable).
				Order("slot_id").
				Scan(&granted).Error; err != nil {
				return fmt.Errorf("read claimed slot ownership: %w", err)
			}
		}
		outcomes = buildClaimOutcomes(request, locked, granted, now)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return outcomes, nil
}

// readClaimOutcomes reports every requested slot, using the pre-update rows for
// refusals so the caller learns when a takeover may be retried.
func (d *ownershipData) readClaimOutcomes(
	ctx context.Context,
	tx *gorm.DB,
	request biz.ClaimRequest,
	granted []ownershipRow,
) ([]biz.ClaimOutcome, error) {
	grantedBySlot := make(map[uint32]ownershipRow, len(granted))
	for _, row := range granted {
		grantedBySlot[row.SlotID] = row
	}
	refused := make([]uint32, 0, len(request.Slots))
	for _, slotID := range request.Slots {
		if _, ok := grantedBySlot[slotID]; !ok {
			refused = append(refused, slotID)
		}
	}
	current := make([]ownershipRow, 0, len(refused))
	if len(refused) > 0 {
		if err := tx.WithContext(ctx).Table("slot_ownership").
			Select(ownershipColumns).
			Where("slot_id IN ?", refused).
			Order("slot_id").
			Scan(&current).Error; err != nil {
			return nil, fmt.Errorf("read refused slot ownership: %w", err)
		}
	}
	return buildClaimOutcomes(request, current, granted, time.Now()), nil
}

func buildClaimOutcomes(
	request biz.ClaimRequest,
	previous []ownershipRow,
	granted []ownershipRow,
	_ time.Time,
) []biz.ClaimOutcome {
	grantedBySlot := make(map[uint32]ownershipRow, len(granted))
	for _, row := range granted {
		grantedBySlot[row.SlotID] = row
	}
	previousBySlot := make(map[uint32]ownershipRow, len(previous))
	for _, row := range previous {
		previousBySlot[row.SlotID] = row
	}
	outcomes := make([]biz.ClaimOutcome, 0, len(request.Slots))
	for _, slotID := range request.Slots {
		if row, ok := grantedBySlot[slotID]; ok {
			outcomes = append(outcomes, biz.ClaimOutcome{
				Ownership: row.ownership(),
				Granted:   true,
			})
			continue
		}
		row := previousBySlot[slotID]
		outcome := biz.ClaimOutcome{Ownership: row.ownership()}
		if row.GrantedAt != nil {
			outcome.NotBefore = row.GrantedAt.Add(request.QuietWindow)
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func (d *ownershipData) RenewSlots(
	ctx context.Context,
	request biz.RenewRequest,
) ([]biz.Ownership, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("sequence ownership: database is required")
	}
	if len(request.Groups) == 0 {
		return nil, nil
	}
	dialect := d.db.Name()
	now, err := StorageNowExpression(dialect)
	if err != nil {
		return nil, err
	}
	var renewed []biz.Ownership
	err = d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, group := range request.Groups {
			if group.InstanceID == "" {
				return errors.New("sequence ownership: instance id is required to renew slots")
			}
			for start := 0; start < len(group.Slots); start += statementChunkSize {
				end := min(start+statementChunkSize, len(group.Slots))
				chunk, renewErr := renewSlotChunk(
					ctx, tx, dialect, now, group.InstanceID, group.Epoch,
					group.Slots[start:end],
				)
				if renewErr != nil {
					return renewErr
				}
				renewed = append(renewed, chunk...)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return renewed, nil
}

// renewSlotChunk renews one batch of slots that share an instance and epoch, and
// returns the rows it actually renewed.
//
// The epoch-CAS predicate is the whole safety property: a row whose owner or
// epoch moved since the caller read it is left alone, so a renewal can never
// resurrect authority a takeover already replaced. Only rows the caller still
// owns at the epoch it presented come back; a slot missing from the result is one
// a takeover replaced, and the caller fences it rather than assuming the renewal
// happened. The caller splits a whole-space renewal so a statement carries a
// bounded number of bind parameters and locks.
func renewSlotChunk(
	ctx context.Context,
	tx *gorm.DB,
	dialect string,
	now string,
	instanceID string,
	epoch uint64,
	slots []uint32,
) ([]biz.Ownership, error) {
	if dialect == "postgres" {
		// One statement both renews and reports which rows still matched, so the
		// takeovers this renewal lost to are exactly the slots it omits.
		query := "UPDATE slot_ownership SET granted_at = " + now +
			", updated_at = " + now + " " +
			"WHERE state = 'OWNED' AND owner_instance_id = ? AND epoch = ? " +
			"AND slot_id IN ? RETURNING " + ownershipColumns
		var rows []ownershipRow
		if err := tx.WithContext(ctx).
			Raw(query, instanceID, int64(epoch), slots).
			Scan(&rows).Error; err != nil {
			return nil, fmt.Errorf("renew slot ownership: %w", err)
		}
		return ownershipRows(rows), nil
	}
	// MySQL has no UPDATE ... RETURNING, and SQLite shares the same transaction
	// shape in the component tests, so the matching rows are locked and read
	// first and the update then touches exactly those. Locking is what
	// serialises renewal against a concurrent takeover.
	var locked []ownershipRow
	lockQuery := tx.WithContext(ctx).Table("slot_ownership").
		Select(ownershipColumns).
		Where(
			"state = ? AND owner_instance_id = ? AND epoch = ? AND slot_id IN ?",
			string(biz.SlotOwned), instanceID, epoch, slots,
		).
		Order("slot_id")
	if dialect != "sqlite" {
		lockQuery = lockQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := lockQuery.Scan(&locked).Error; err != nil {
		return nil, fmt.Errorf("lock slot ownership for renewal: %w", err)
	}
	if len(locked) == 0 {
		return nil, nil
	}
	renewable := make([]uint32, 0, len(locked))
	for _, row := range locked {
		renewable = append(renewable, row.SlotID)
	}
	if err := tx.WithContext(ctx).Table("slot_ownership").
		Where("slot_id IN ?", renewable).
		Updates(map[string]any{
			"granted_at": gorm.Expr(now),
			"updated_at": gorm.Expr(now),
		}).Error; err != nil {
		return nil, fmt.Errorf("renew slot ownership: %w", err)
	}
	return ownershipRows(locked), nil
}

// ownershipRows converts authority rows to the domain shape.
func ownershipRows(rows []ownershipRow) []biz.Ownership {
	renewed := make([]biz.Ownership, 0, len(rows))
	for _, row := range rows {
		renewed = append(renewed, row.ownership())
	}
	return renewed
}

func (d *ownershipData) ReleaseSlots(
	ctx context.Context,
	targets []biz.SlotAuthority,
) (int64, error) {
	if d == nil || d.db == nil {
		return 0, errors.New("sequence ownership: database is required")
	}
	if len(targets) == 0 {
		return 0, nil
	}
	dialect := d.db.Name()
	now, err := StorageNowExpression(dialect)
	if err != nil {
		return 0, err
	}
	var released int64
	err = d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for start := 0; start < len(targets); start += statementChunkSize {
			end := min(start+statementChunkSize, len(targets))
			count, releaseErr := releaseSlotChunk(ctx, tx, dialect, now, targets[start:end])
			if releaseErr != nil {
				return releaseErr
			}
			released += count
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return released, nil
}

// releaseSlotChunk revokes one batch of slot authorities.
//
// The epoch-CAS predicate is the whole safety property of a release: writing
// UNOWNED without matching the instance and the epoch would silently steal the
// slot from a newer owner (section 6.5).
func releaseSlotChunk(
	ctx context.Context,
	tx *gorm.DB,
	dialect string,
	now string,
	targets []biz.SlotAuthority,
) (int64, error) {
	if dialect == "postgres" {
		// Each parameter carries an explicit cast. PostgreSQL infers a
		// parameter-only VALUES column as text, which would break the integer
		// comparison; the cast also tells the driver to send the slot id as a
		// number rather than a string. The Go arguments are int64 because that is
		// what the driver can encode for these parameter types.
		values := make([]string, 0, len(targets))
		args := make([]any, 0, len(targets)*3)
		for _, target := range targets {
			values = append(values, "(?::integer, ?::text, ?::bigint)")
			args = append(args, int64(target.SlotID), target.InstanceID, int64(target.Epoch))
		}
		query := "UPDATE slot_ownership AS o SET " +
			"state = 'UNOWNED', owner_node_id = NULL, owner_instance_id = NULL, " +
			"granted_at = NULL, updated_at = " + now + " " +
			"FROM (VALUES " + strings.Join(values, ", ") + ") " +
			"AS t(slot_id, instance_id, epoch) " +
			"WHERE o.slot_id = t.slot_id AND o.state = 'OWNED' " +
			"AND o.owner_instance_id = t.instance_id AND o.epoch = t.epoch " +
			"RETURNING " + ownershipColumnsQualified
		var revoked []ownershipRow
		if err := tx.Raw(query, args...).Scan(&revoked).Error; err != nil {
			return 0, fmt.Errorf("release slot ownership: %w", err)
		}
		return int64(len(revoked)), nil
	}

	values := make([]string, 0, len(targets))
	args := make([]any, 0, len(targets)*3)
	for _, target := range targets {
		values = append(values, "(?, ?, ?)")
		args = append(args, int64(target.SlotID), target.InstanceID, int64(target.Epoch))
	}
	list := strings.Join(values, ", ")

	// MySQL has no UPDATE ... RETURNING, so read the rows the predicate actually
	// matches first, under a lock, and then revoke exactly those.
	lockQuery := "SELECT " + ownershipColumns + " FROM slot_ownership " +
		"WHERE state = 'OWNED' AND (slot_id, owner_instance_id, epoch) IN (" + list + ")"
	if dialect != "sqlite" {
		lockQuery += " FOR UPDATE"
	}
	var matched []ownershipRow
	if err := tx.Raw(lockQuery, args...).Scan(&matched).Error; err != nil {
		return 0, fmt.Errorf("read released slot ownership: %w", err)
	}
	if len(matched) == 0 {
		return 0, nil
	}
	update := "UPDATE slot_ownership SET " +
		"state = 'UNOWNED', owner_node_id = NULL, owner_instance_id = NULL, " +
		"granted_at = NULL, updated_at = " + now + " " +
		"WHERE state = 'OWNED' AND (slot_id, owner_instance_id, epoch) IN (" + list + ")"
	if err := tx.Exec(update, args...).Error; err != nil {
		return 0, fmt.Errorf("release slot ownership: %w", err)
	}
	return int64(len(matched)), nil
}
