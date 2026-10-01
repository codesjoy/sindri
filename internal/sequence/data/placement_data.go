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
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	gormio "gorm.io/gorm"
)

// livenessRow is one node's liveness lease as this process reads it.
type livenessRow struct {
	NodeID     string    `gorm:"column:node_id"`
	InstanceID string    `gorm:"column:instance_id"`
	LastSeenAt time.Time `gorm:"column:last_seen_at"`
}

// PlacementData is the storage side of the placement tables: the view a node
// plans from, and the directory revision the publisher writes.
//
// It is one type rather than two because both halves read the same rows through
// the same dialect expressions. A second copy would be a second place for the
// ownership age or the storage clock to be spelled differently, and the two
// would then disagree about when a takeover is admissible.
type PlacementData struct {
	db *gormio.DB
}

// NewPlacementData constructs the placement repository backed by db.
func NewPlacementData(db *gormio.DB) *PlacementData {
	return &PlacementData{db: db}
}

// OwnershipView reads every slot's authority, ordered by slot id, with the age
// of each grant.
//
// The age is selected with the shared per-dialect expression so the node's read
// and the publisher's read agree: only the authority's own clock can say whether
// a grant has lapsed past the quiet window.
func (d *PlacementData) OwnershipView(ctx context.Context) ([]biz.Ownership, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("sequence placement: database is required")
	}
	age, err := OwnershipAgeExpression(d.db.Name())
	if err != nil {
		return nil, err
	}
	var rows []ownershipRow
	if err := d.db.WithContext(ctx).
		Table("slot_ownership").
		Select(ownershipColumns + ", " + age + " AS granted_ago_micros").
		Order("slot_id").
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("read slot ownership view: %w", err)
	}
	view := make([]biz.Ownership, 0, len(rows))
	for _, row := range rows {
		view = append(view, row.ownership())
	}
	return view, nil
}

// ownershipSegmentRow is one aggregated authority run as the segment query
// returns it: a contiguous range of slot ids that share one owner story.
type ownershipSegmentRow struct {
	StartSlot       uint32  `gorm:"column:start_slot"`
	EndSlot         uint32  `gorm:"column:end_slot"`
	OwnerNodeID     *string `gorm:"column:owner_node_id"`
	OwnerInstanceID *string `gorm:"column:owner_instance_id"`
	Epoch           uint64  `gorm:"column:epoch"`
	State           string  `gorm:"column:state"`
	// GrantAgeKnown is 1 when the run's boundary row carried a grant time.
	GrantAgeKnown int64 `gorm:"column:grant_age_known"`
	Overdue       int64 `gorm:"column:overdue"`
}

// OwnershipSegments reads the compact authority view: contiguous runs of slots
// sharing one owner, instance, epoch, state and quiet-window classification,
// with each run's grant age measured by the storage clock.
//
// The aggregation is what keeps a hundred-node control plane's read
// proportional to the number of runs rather than to SlotCount: at steady state
// the answer is one or two runs per live node, not 16,384 rows. The viewer is
// still served by this process's own connection, so it reads whichever server
// that connection names -- production wiring points it at the primary, because
// the grant age the quiet window is judged against must not be a replica's.
func (d *PlacementData) OwnershipSegments(
	ctx context.Context,
	quietWindow time.Duration,
) ([]biz.OwnershipSegment, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("sequence placement: database is required")
	}
	if quietWindow < 0 {
		return nil, errors.New("sequence placement: quiet window must not be negative")
	}
	age, err := OwnershipAgeExpression(d.db.Name())
	if err != nil {
		return nil, err
	}
	statement, err := ownershipSegmentsStatement(d.db.Name(), age)
	if err != nil {
		return nil, err
	}
	var rows []ownershipSegmentRow
	if err := d.db.WithContext(ctx).
		Raw(statement, quietWindow.Microseconds()).
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("read slot ownership segments: %w", err)
	}
	segments := make([]biz.OwnershipSegment, 0, len(rows))
	for _, row := range rows {
		segment := biz.OwnershipSegment{
			StartSlot:          row.StartSlot,
			EndSlot:            row.EndSlot,
			Epoch:              row.Epoch,
			State:              biz.SlotState(row.State),
			GrantAgeKnown:      row.GrantAgeKnown == 1,
			QuietWindowOverdue: row.Overdue == 1,
		}
		if row.OwnerNodeID != nil {
			segment.OwnerNodeID = *row.OwnerNodeID
		}
		if row.OwnerInstanceID != nil {
			segment.OwnerInstanceID = *row.OwnerInstanceID
		}
		segments = append(segments, segment)
	}
	if err := biz.ValidateOwnershipSegments(segments); err != nil {
		return nil, err
	}
	return segments, nil
}

// ownershipSegmentsStatement builds the gaps-and-islands aggregation.
//
// The island key is "slot id minus the row number within this partition", which
// is constant exactly for consecutive slot ids that share every attribute in
// the partition. Each dialect spells the cast differently only because a
// subtraction of two unsigned integers would otherwise be the one expression
// that overflows here.
func ownershipSegmentsStatement(dialect, age string) (string, error) {
	var island string
	const partition = "PARTITION BY owner_node_id, owner_instance_id, epoch, state, " +
		"grant_age_known, overdue ORDER BY slot_id"
	switch dialect {
	case "postgres":
		island = "CAST(slot_id AS bigint) - ROW_NUMBER() OVER (" + partition + ")"
	case "mysql":
		// MySQL's ROW_NUMBER() is BIGINT UNSIGNED, and signed-minus-unsigned is
		// evaluated in the unsigned domain, so the first row of the space would
		// fail with an out-of-range error instead of producing the run key. Both
		// sides are cast to SIGNED so the subtraction is the signed one the
		// island key needs.
		island = "CAST(slot_id AS SIGNED) - " +
			"CAST(ROW_NUMBER() OVER (" + partition + ") AS SIGNED)"
	case "sqlite":
		island = "slot_id - ROW_NUMBER() OVER (" + partition + ")"
	default:
		return "", fmt.Errorf("sequence placement: unsupported dialect %q", dialect)
	}
	return "WITH classified AS (" +
		"SELECT slot_id, " +
		"CASE WHEN state = 'OWNED' THEN owner_node_id ELSE NULL END AS owner_node_id, " +
		"CASE WHEN state = 'OWNED' THEN owner_instance_id ELSE NULL END AS owner_instance_id, " +
		"CASE WHEN state = 'OWNED' THEN epoch ELSE 0 END AS epoch, " +
		"state, " +
		"CASE WHEN granted_at IS NOT NULL THEN 1 ELSE 0 END AS grant_age_known, " +
		"CASE WHEN state = 'OWNED' AND granted_at IS NOT NULL AND (" + age + ") >= ? " +
		"THEN 1 ELSE 0 END AS overdue " +
		"FROM slot_ownership" +
		"), islands AS (" +
		"SELECT classified.*, " + island + " AS island FROM classified" +
		") SELECT MIN(slot_id) AS start_slot, MAX(slot_id) AS end_slot, " +
		"owner_node_id, owner_instance_id, epoch, state, " +
		"MIN(grant_age_known) AS grant_age_known, MIN(overdue) AS overdue " +
		"FROM islands GROUP BY island, owner_node_id, owner_instance_id, epoch, state, " +
		"grant_age_known, overdue " +
		"ORDER BY start_slot", nil
}

// LiveNodes returns the nodes whose liveness lease has not lapsed.
//
// The cutoff is a storage-clock expression rather than an instant this process
// computed, so the comparison happens in the authority's time on both sides: a
// reader-supplied timestamp would put the reader host's clock into a membership
// decision the storage alone can adjudicate.
//
// A lapsed row is left where it is. Nothing here deletes it, and nothing needs
// to: node_id is the primary key, so the table is bounded by the fleet size.
func (d *PlacementData) LiveNodes(
	ctx context.Context,
	ttl time.Duration,
) ([]biz.NodeInfo, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("sequence placement: database is required")
	}
	if ttl <= 0 {
		return nil, errors.New("sequence placement: node ttl must be positive")
	}
	cutoff, err := clockOffsetExpression(d.db.Name(), -ttl)
	if err != nil {
		return nil, err
	}
	var rows []livenessRow
	if err := d.db.WithContext(ctx).
		Table("sequence_node_liveness").
		Select("node_id, instance_id, last_seen_at").
		Where("last_seen_at >= " + cutoff).
		Order("node_id").
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("read live nodes: %w", err)
	}
	live := make([]biz.NodeInfo, 0, len(rows))
	for _, row := range rows {
		live = append(live, biz.NodeInfo{
			ID:         row.NodeID,
			InstanceID: row.InstanceID,
			LastSeenAt: row.LastSeenAt,
		})
	}
	return live, nil
}

// AcquireCoordinator takes the publisher lease when it is free or has lapsed,
// and reports this instance's tenure afterwards.
//
// The row is taken when nobody holds it, when the holder's lease has lapsed, or
// when the caller already holds it, and the epoch only moves when the role
// actually changes hands. The epoch is read back rather than assumed, because it
// is what the publish presents as its credential. MySQL has no UPDATE ...
// RETURNING, so the read is a second statement in the same transaction, where
// the row is already locked.
func (d *PlacementData) AcquireCoordinator(
	ctx context.Context,
	instanceID string,
	lease time.Duration,
) (biz.CoordinatorLease, error) {
	if d == nil || d.db == nil {
		return biz.CoordinatorLease{}, errors.New("sequence placement: database is required")
	}
	if instanceID == "" {
		return biz.CoordinatorLease{}, errors.New("sequence placement: instance id is required")
	}
	if lease <= 0 {
		return biz.CoordinatorLease{}, errors.New(
			"sequence placement: coordinator lease must be positive",
		)
	}
	expiry, err := clockOffsetExpression(d.db.Name(), lease)
	if err != nil {
		return biz.CoordinatorLease{}, err
	}
	now, err := StorageNowExpression(d.db.Name())
	if err != nil {
		return biz.CoordinatorLease{}, err
	}
	held := biz.CoordinatorLease{InstanceID: instanceID}
	err = d.db.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
		// epoch is assigned before owner_instance_id deliberately. A single-table
		// MySQL UPDATE evaluates its assignments from left to right and a later
		// expression sees the earlier assignment, so an epoch computed after the
		// owner had been overwritten would compare the new owner against itself
		// and never move the fencing token. Postgres evaluates every assignment
		// against the row as it was, so the order only decides the answer on
		// MySQL -- and there it is what makes the token move at all.
		update := "UPDATE sequence_coordinator SET " +
			"epoch = epoch + CASE WHEN owner_instance_id IS NULL OR " +
			"owner_instance_id <> ? THEN 1 ELSE 0 END, " +
			"owner_instance_id = ?, " +
			"expires_at = " + expiry + ", updated_at = " + now + " " +
			"WHERE id = 1 AND (owner_instance_id IS NULL OR " +
			"owner_instance_id = ? OR expires_at IS NULL OR expires_at < " + now + ")"
		result := tx.Exec(update, instanceID, instanceID, instanceID)
		if result.Error != nil {
			return fmt.Errorf("acquire coordinator lease: %w", result.Error)
		}
		held.Held = result.RowsAffected > 0
		if !held.Held {
			return nil
		}
		var row struct {
			Epoch uint64 `gorm:"column:epoch"`
		}
		if err := tx.Raw(
			"SELECT epoch FROM sequence_coordinator WHERE id = 1",
		).Scan(&row).Error; err != nil {
			return fmt.Errorf("read coordinator epoch: %w", err)
		}
		held.Epoch = row.Epoch
		return nil
	})
	if err != nil {
		return biz.CoordinatorLease{}, err
	}
	return held, nil
}

// MaterialiseRoute publishes a snapshot of the compact ownership view.
//
// The revision comes from sequence_route_state, which is seeded above every
// version already published, so a materialised snapshot can never carry a
// version a client has already moved past. A view identical to the one already
// published returns the existing revision without writing: routes only advance
// when ownership changed, which keeps a stable fleet from growing the route
// table and from telling every client to refresh on a timer.
//
// Every publish -- including one that matched an identical snapshot -- prunes
// revisions older than the retention bound in the same transaction. The bound
// is enforced on the write rather than by a maintenance job so the table can
// never exceed it, even if no pass ever changes the payload again.
//
// The caller's tenure is locked and confirmed before anything is published, so a
// pass that lost the role cannot push a directory the fleet has moved past. The
// revision advance and the insert are in that same transaction, which is what
// keeps the published revision monotonic under the one publisher that may write
// it.
func (d *PlacementData) MaterialiseRoute(
	ctx context.Context,
	segments []biz.OwnershipSegment,
	layoutVersion int64,
	retention int,
	lease biz.CoordinatorLease,
) (biz.PublishResult, error) {
	if d == nil || d.db == nil {
		return biz.PublishResult{}, errors.New("sequence placement: database is required")
	}
	if retention <= 0 {
		return biz.PublishResult{}, errors.New(
			"sequence placement: route retention must be positive",
		)
	}
	payload, err := biz.EncodeOwnershipSegments(segments, layoutVersion)
	if err != nil {
		return biz.PublishResult{}, err
	}
	now, err := StorageNowExpression(d.db.Name())
	if err != nil {
		return biz.PublishResult{}, err
	}
	result := biz.PublishResult{PayloadBytes: len(payload)}
	err = d.db.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
		if err := GuardCoordinator(ctx, tx, d.db.Name(), lease); err != nil {
			return err
		}
		var latest RouteModel
		readErr := tx.Order("version DESC").Take(&latest).Error
		switch {
		case readErr == nil:
			// The comparison is made on what the payload means, not on its
			// bytes: a jsonb column re-serialises what it stores, so identical
			// directories do not come back byte-identical.
			if biz.SameRoutePayload(latest.Payload, payload) {
				result.Revision = latest.Version
				break
			}
			if err := writeMaterialisedRoute(
				ctx,
				tx,
				d.db.Name(),
				now,
				payload,
				&result,
			); err != nil {
				return err
			}
		case errors.Is(readErr, gormio.ErrRecordNotFound):
			if err := writeMaterialisedRoute(
				ctx,
				tx,
				d.db.Name(),
				now,
				payload,
				&result,
			); err != nil {
				return err
			}
		default:
			return fmt.Errorf("read latest route: %w", readErr)
		}
		deleted, pruneErr := pruneRoutes(ctx, tx, result.Revision, retention)
		if pruneErr != nil {
			return pruneErr
		}
		result.RetentionDeleted = deleted
		return nil
	})
	if err != nil {
		return biz.PublishResult{}, err
	}
	return result, nil
}

// writeMaterialisedRoute advances the directory revision and inserts the
// snapshot that carries it.
func writeMaterialisedRoute(
	ctx context.Context,
	tx *gormio.DB,
	dialect string,
	now string,
	payload []byte,
	result *biz.PublishResult,
) error {
	next, err := AdvanceRouteRevision(ctx, tx, dialect)
	if err != nil {
		return err
	}
	if err := tx.WithContext(ctx).Exec(
		"INSERT INTO sequence_routes (version, payload, created_at) VALUES (?, ?, "+now+")",
		next,
		payload,
	).Error; err != nil {
		return fmt.Errorf("insert materialised route: %w", err)
	}
	result.Revision = next
	return nil
}

// pruneRoutes deletes every revision older than the retention bound, counted
// back from the newest one this publish is leaving in place.
func pruneRoutes(
	ctx context.Context,
	tx *gormio.DB,
	revision int64,
	retention int,
) (int64, error) {
	oldestKept := revision - int64(retention)
	if oldestKept < 1 {
		return 0, nil
	}
	result := tx.WithContext(ctx).Exec(
		"DELETE FROM sequence_routes WHERE version <= ?",
		oldestKept,
	)
	if result.Error != nil {
		return 0, fmt.Errorf("prune materialised routes: %w", result.Error)
	}
	return result.RowsAffected, nil
}

// clockOffsetExpression returns the dialect expression for the storage clock
// moved by offset: positive for a future instant, negative for a past one.
//
// It is written once because two different questions ask it with opposite signs
// -- when does a tenure end, and how old may a liveness renewal be -- and each
// dialect spells the arithmetic differently.
func clockOffsetExpression(dialect string, offset time.Duration) (string, error) {
	switch dialect {
	case "postgres":
		return fmt.Sprintf(
			"clock_timestamp() + make_interval(secs => %f)",
			offset.Seconds(),
		), nil
	case "mysql":
		return fmt.Sprintf(
			"CURRENT_TIMESTAMP(6) + INTERVAL %d MICROSECOND",
			offset.Microseconds(),
		), nil
	case "sqlite":
		return fmt.Sprintf(
			"strftime('%%Y-%%m-%%d %%H:%%M:%%f', 'now', '%+.6f seconds')",
			offset.Seconds(),
		), nil
	default:
		return "", fmt.Errorf("sequence placement: unsupported dialect %q", dialect)
	}
}

// livenessData stores the live node set on the same database as slot ownership.
type livenessData struct {
	db *gormio.DB
}

// NewLivenessData returns the storage-backed liveness record.
func NewLivenessData(db *gormio.DB) biz.LivenessRepo {
	return &livenessData{db: db}
}

// RenewLiveness writes this node's row with the storage clock as its timestamp.
//
// One statement covers both cases: a node reporting for the first time inserts,
// and one that is already recorded updates in place, so the table holds one row
// per node rather than one per report. The upsert also keeps a transient
// duplicate from failing the heartbeat: two reports racing at the same instant
// both succeed, and the newest timestamp wins.
func (d *livenessData) RenewLiveness(ctx context.Context, nodeID, instanceID string) error {
	if d == nil || d.db == nil {
		return errors.New("sequence liveness: database is required")
	}
	now, err := StorageNowExpression(d.db.Name())
	if err != nil {
		return err
	}
	statement := renewLivenessStatement(d.db.Name(), now)
	if err := d.db.WithContext(ctx).
		Exec(statement, nodeID, instanceID).Error; err != nil {
		return fmt.Errorf("renew node liveness: %w", err)
	}
	return nil
}

// DropLiveness removes this node's row if it is still the one recorded.
//
// The instance id in the predicate is what makes a shutdown race safe: a process
// that is shutting down while its replacement has already registered leaves the
// new row alone, so a restart cannot be erased by the process it replaced. A row
// this instance did not write is not this process's to delete.
func (d *livenessData) DropLiveness(ctx context.Context, nodeID, instanceID string) error {
	if d == nil || d.db == nil {
		return errors.New("sequence liveness: database is required")
	}
	if err := d.db.WithContext(ctx).
		Exec(
			"DELETE FROM sequence_node_liveness WHERE node_id = ? AND instance_id = ?",
			nodeID, instanceID,
		).Error; err != nil {
		return fmt.Errorf("drop node liveness: %w", err)
	}
	return nil
}

// renewLivenessStatement builds the dialect's single upsert.
//
// PostgreSQL cannot infer a type for a bare parameter in a VALUES column, so its
// two are cast; MySQL has no ON CONFLICT form and updates through its own upsert
// clause instead. The timestamp is written by the storage clock in every dialect,
// for the reason the table's comment gives: the reader compares it against a TTL.
func renewLivenessStatement(dialect, now string) string {
	switch dialect {
	case "postgres":
		return "INSERT INTO sequence_node_liveness (node_id, instance_id, last_seen_at) " +
			"VALUES (?::varchar, ?::varchar, " + now + ") " +
			"ON CONFLICT (node_id) DO UPDATE SET " +
			"instance_id = EXCLUDED.instance_id, last_seen_at = EXCLUDED.last_seen_at"
	case "mysql":
		return "INSERT INTO sequence_node_liveness (node_id, instance_id, last_seen_at) " +
			"VALUES (?, ?, " + now + ") " +
			"ON DUPLICATE KEY UPDATE " +
			"instance_id = VALUES(instance_id), last_seen_at = " + now
	default:
		// SQLite, used by component tests, accepts the PostgreSQL form.
		return "INSERT INTO sequence_node_liveness (node_id, instance_id, last_seen_at) " +
			"VALUES (?, ?, " + now + ") " +
			"ON CONFLICT (node_id) DO UPDATE SET " +
			"instance_id = EXCLUDED.instance_id, last_seen_at = EXCLUDED.last_seen_at"
	}
}

// StorageNowExpression returns the dialect expression for the current instant on
// the storage clock.
func StorageNowExpression(dialect string) (string, error) {
	switch dialect {
	case "postgres":
		return "clock_timestamp()", nil
	case "mysql":
		return "CURRENT_TIMESTAMP(6)", nil
	case "sqlite":
		return "CURRENT_TIMESTAMP", nil
	default:
		return "", fmt.Errorf("sequence ownership: unsupported dialect %q", dialect)
	}
}

// OwnershipAgeExpression returns the dialect expression for how long ago the row's
// granted_at was set, measured by the storage clock in the statement that reads
// it.
//
// The age must be read from the database rather than derived from a node's own
// clock: the quiet window gates a takeover on storage time, and a comparison
// between a node reading and a stored instant is exactly the dependency this
// read exists to avoid.
func OwnershipAgeExpression(dialect string) (string, error) {
	switch dialect {
	case "postgres":
		return "CAST(EXTRACT(EPOCH FROM (clock_timestamp() - granted_at)) * 1000000 AS bigint)", nil
	case "mysql":
		return "TIMESTAMPDIFF(MICROSECOND, granted_at, CURRENT_TIMESTAMP(6))", nil
	case "sqlite":
		// SQLite has no fractional-second CURRENT_TIMESTAMP, so the values this
		// dialect stores are whole seconds and the reading is truncated to match.
		// julianday is used because it is the only fractional-precision form
		// available here, but comparing it against a second-precision column
		// would report a just-granted row as up to a second old.
		return "CAST((julianday(strftime('%Y-%m-%d %H:%M:%S', 'now')) - " +
			"julianday(granted_at)) * 86400000000 AS INTEGER)", nil
	default:
		return "", fmt.Errorf("sequence ownership: unsupported dialect %q", dialect)
	}
}

// GuardCoordinator locks the coordinator row for the rest of the caller's
// transaction and confirms the tenure it was given is still the one in force.
//
// It is what makes a placement write a write *under a tenure* rather than merely
// after acquiring one. The writer acquires the role at the start of a pass and may
// spend a while planning, and this is the check that stops a pass that overran --
// or one that was simply slow enough for another replica's takeover to commit --
// from publishing a decision on behalf of a role it no longer holds. It is the
// publisher's write path that runs it, and the predicate lives here so the write
// and the takeover it races are defined against the same state.
//
// The lock, not the comparison, is the point. Taking it as a write keeps the row
// locked until the caller commits, so a replica taking the role over cannot commit
// the takeover in the middle of a write made under the old tenure; a plain read
// would leave exactly that window open. The update deliberately does not renew
// expires_at: a pass that has run out of lease has to stop, not buy more time.
//
// The lock and the comparison are two statements because MySQL reports *changed*
// rows rather than matched ones, so a renewal that changed nothing would look like
// a refusal. The lock statement carries the full predicate anyway, so a stale
// tenure does not touch the row; whether it matched is decided by the read that
// follows it, which is the authoritative one.
func GuardCoordinator(
	ctx context.Context,
	tx *gormio.DB,
	dialect string,
	lease biz.CoordinatorLease,
) error {
	now, err := StorageNowExpression(dialect)
	if err != nil {
		return err
	}
	// "Still mine" is deliberately not the negation of AcquireCoordinator's
	// predicate: that negation says "someone else holds it and it has not
	// expired", which is precisely the state a takeover may commit from. This
	// predicate instead names the state a takeover cannot reach: the row still
	// carries my instance and my epoch, and has not run out. A stale tenure fails
	// it, and it is the two together that let a writer and a takeover exclude each
	// other.
	held := "owner_instance_id = ? AND epoch = ? " +
		"AND expires_at IS NOT NULL AND expires_at >= " + now
	if err := tx.WithContext(ctx).Exec(
		"UPDATE sequence_coordinator SET updated_at = "+now+" WHERE id = 1 AND "+held,
		lease.InstanceID, lease.Epoch,
	).Error; err != nil {
		return fmt.Errorf("lock coordinator lease: %w", err)
	}
	var row struct {
		Held *bool `gorm:"column:held"`
	}
	if err := tx.WithContext(ctx).Raw(
		"SELECT ("+held+") AS held FROM sequence_coordinator WHERE id = 1",
		lease.InstanceID, lease.Epoch,
	).Scan(&row).Error; err != nil {
		return fmt.Errorf("verify coordinator lease: %w", err)
	}
	if row.Held == nil || !*row.Held {
		return biz.ErrCoordinatorLost
	}
	return nil
}
