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
	"gorm.io/gorm"
)

// PlacementData reads the fleet's ownership, membership and route snapshot.
type PlacementData struct {
	db      *gorm.DB
	lease   time.Duration
	nodeTTL time.Duration
}

// NewPlacementData builds the placement reader over db.
func NewPlacementData(db *gorm.DB, lease, nodeTTL time.Duration) *PlacementData {
	return &PlacementData{db: db, lease: lease, nodeTTL: nodeTTL}
}

type ownershipSegmentRow struct {
	StartSlot       uint32
	EndSlot         uint32
	OwnerNodeID     string
	OwnerInstanceID string
	Epoch           uint64
	State           string
	GrantAgeKnown   bool
	Overdue         bool
	ReleaseReady    bool
}

// OwnershipSegments reads the complete ownership view as contiguous segments,
// classified by lease age and release readiness.
func (d *PlacementData) OwnershipSegments(
	ctx context.Context,
	quietWindow time.Duration,
) ([]biz.OwnershipSegment, error) {
	age, err := elapsedExpression(d.db.Name(), "i.granted_at")
	if err != nil {
		return nil, err
	}
	cast := "CAST(slot_id AS bigint)"
	if d.db.Name() == "mysql" {
		cast = "CAST(slot_id AS SIGNED)"
	}
	rowNumber := "ROW_NUMBER() OVER (PARTITION BY owner_node_id, owner_instance_id, epoch, state, grant_age_known, overdue, release_ready ORDER BY slot_id)"
	if d.db.Name() == "mysql" {
		rowNumber = "CAST(" + rowNumber + " AS SIGNED)"
	}
	now, err := storageNowExpression(d.db.Name())
	if err != nil {
		return nil, err
	}
	query := "WITH classified AS (SELECT o.slot_id, COALESCE(i.node_id, '') AS owner_node_id, COALESCE(o.owner_instance_id, '') AS owner_instance_id, o.epoch, o.state, CASE WHEN i.granted_at IS NULL THEN 0 ELSE 1 END AS grant_age_known, CASE WHEN i.granted_at IS NOT NULL AND " + age + " >= ? THEN 1 ELSE 0 END AS overdue, CASE WHEN o.state='DRAINING' AND h.kind='RELEASE' AND h.phase='DRAINING' AND h.source_instance_id=o.owner_instance_id AND h.source_epoch=o.epoch AND (h.drained OR h.not_before <= " + now + ") THEN 1 ELSE 0 END AS release_ready FROM sequence_slot_ownership o LEFT JOIN sequence_instance_leases i ON o.owner_instance_id = i.instance_id LEFT JOIN sequence_slot_handoffs h ON h.slot_id=o.slot_id), islands AS (SELECT *, " + cast + " - " + rowNumber + " AS island FROM classified) SELECT MIN(slot_id) AS start_slot, MAX(slot_id) AS end_slot, owner_node_id, owner_instance_id, epoch, state, grant_age_known, overdue, release_ready FROM islands GROUP BY owner_node_id, owner_instance_id, epoch, state, grant_age_known, overdue, release_ready, island ORDER BY start_slot"
	var rows []ownershipSegmentRow
	if err := d.db.WithContext(ctx).
		Raw(query, quietWindow.Microseconds()).
		Scan(&rows).
		Error; err != nil {
		return nil, err
	}
	segments := make([]biz.OwnershipSegment, 0, len(rows))
	for _, r := range rows {
		segments = append(
			segments,
			biz.OwnershipSegment{
				StartSlot:          r.StartSlot,
				EndSlot:            r.EndSlot,
				OwnerNodeID:        r.OwnerNodeID,
				OwnerInstanceID:    r.OwnerInstanceID,
				Epoch:              r.Epoch,
				State:              biz.SlotState(r.State),
				GrantAgeKnown:      r.GrantAgeKnown,
				QuietWindowOverdue: r.Overdue,
				ReleaseReady:       r.ReleaseReady,
			},
		)
	}
	return segments, biz.ValidateOwnershipSegments(segments)
}

// LiveNodes returns the members whose lease and liveness rows are both fresh.
func (d *PlacementData) LiveNodes(ctx context.Context, ttl time.Duration) ([]biz.NodeInfo, error) {
	seen, err := elapsedExpression(d.db.Name(), "n.last_seen_at")
	if err != nil {
		return nil, err
	}
	grant, err := elapsedExpression(d.db.Name(), "i.granted_at")
	if err != nil {
		return nil, err
	}
	stable, err := elapsedExpression(d.db.Name(), "n.eligible_since")
	if err != nil {
		return nil, err
	}
	var rows []struct {
		NodeID       string
		InstanceID   string
		StableMicros int64
	}
	err = d.db.WithContext(ctx).
		Raw("SELECT n.node_id, n.instance_id, "+stable+" AS stable_micros FROM sequence_node_liveness n JOIN sequence_instance_leases i ON n.instance_id = i.instance_id AND n.node_id = i.node_id WHERE i.state = 'ACTIVE' AND "+seen+" < ? AND "+grant+" < ? ORDER BY n.node_id", ttl.Microseconds(), d.lease.Microseconds()).
		Scan(&rows).
		Error
	nodes := make([]biz.NodeInfo, 0, len(rows))
	for _, r := range rows {
		nodes = append(
			nodes,
			biz.NodeInfo{
				ID:         r.NodeID,
				InstanceID: r.InstanceID,
				StableFor:  time.Duration(r.StableMicros) * time.Microsecond,
			},
		)
	}
	return nodes, err
}

type coordinatorRow struct {
	OwnerInstanceID *string
	Epoch           uint64
	ExpiresAt       *time.Time
}

// AcquireCoordinator takes or renews the control-plane tenure, preserving a
// fresh tenure held by another instance.
func (d *PlacementData) AcquireCoordinator(
	ctx context.Context,
	instanceID string,
	duration time.Duration,
) (biz.CoordinatorLease, error) {
	if instanceID == "" || duration <= 0 {
		return biz.CoordinatorLease{}, errors.New("sequence: invalid coordinator lease")
	}
	lease := biz.CoordinatorLease{InstanceID: instanceID}
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row coordinatorRow
		if err := tx.Table("sequence_coordinator").
			Where("id = 1").
			Clauses(lockClause(tx, "UPDATE")).
			Take(&row).
			Error; err != nil {
			return err
		}
		now, err := readStorageClock(ctx, tx, tx.Name())
		if err != nil {
			return err
		}
		fresh := row.ExpiresAt != nil && now.Before(*row.ExpiresAt)
		self := row.OwnerInstanceID != nil && *row.OwnerInstanceID == instanceID
		if fresh && !self {
			return nil
		}
		if !self || !fresh {
			row.Epoch++
		}
		if err := tx.Table("sequence_coordinator").
			Where("id = 1").
			Updates(map[string]any{"owner_instance_id": instanceID, "epoch": row.Epoch, "expires_at": now.Add(duration), "updated_at": now}).
			Error; err != nil {
			return err
		}
		lease.Held = true
		lease.Epoch = row.Epoch
		return nil
	})
	return lease, err
}

// GuardCoordinator refuses a control-plane write when the tenure is lost or
// does not hold the current coordinator epoch.
func GuardCoordinator(
	ctx context.Context,
	tx *gorm.DB,
	_ string,
	lease biz.CoordinatorLease,
) error {
	if !lease.Held {
		return biz.ErrCoordinatorLost
	}
	var row coordinatorRow
	if err := tx.WithContext(ctx).
		Table("sequence_coordinator").
		Where("id = 1").
		Clauses(lockClause(tx, "UPDATE")).
		Take(&row).
		Error; err != nil {
		return err
	}
	now, err := readStorageClock(ctx, tx, tx.Name())
	if err != nil {
		return err
	}
	if row.OwnerInstanceID == nil || *row.OwnerInstanceID != lease.InstanceID ||
		row.Epoch != lease.Epoch ||
		row.ExpiresAt == nil ||
		!now.Before(*row.ExpiresAt) {
		return biz.ErrCoordinatorLost
	}
	return nil
}

// MaterialiseRoute publishes the ownership view as the single route snapshot,
// bumping the version only when the directory actually changed.
func (d *PlacementData) MaterialiseRoute(
	ctx context.Context,
	segments []biz.OwnershipSegment,
	layout int64,
	lease biz.CoordinatorLease,
) (biz.PublishResult, error) {
	payload, err := biz.EncodeOwnershipSegments(segments, layout)
	if err != nil {
		return biz.PublishResult{}, err
	}
	result := biz.PublishResult{PayloadBytes: len(payload)}
	err = d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := GuardCoordinator(ctx, tx, tx.Name(), lease); err != nil {
			return err
		}
		var current RouteModel
		err := tx.Where("id = 1").Take(&current).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && biz.SameRoutePayload(current.Payload, payload) {
			result.Revision = current.Version
			return nil
		}
		now, err := readStorageClock(ctx, tx, tx.Name())
		if err != nil {
			return err
		}
		result.Revision = current.Version + 1
		current = RouteModel{ID: 1, Version: result.Revision, Payload: payload, UpdatedAt: now}
		if current.Version == 1 {
			return tx.Create(&current).Error
		}
		return tx.Model(&RouteModel{}).
			Where("id = 1").
			Updates(map[string]any{"version": current.Version, "payload": payload, "updated_at": now}).
			Error
	})
	return result, err
}

type livenessData struct {
	db  *gorm.DB
	ttl time.Duration
}

// NewLivenessData builds the membership heartbeat store over db.
func NewLivenessData(db *gorm.DB, ttl time.Duration) biz.LivenessRepo {
	return &livenessData{db: db, ttl: ttl}
}

func (d *livenessData) RenewLiveness(ctx context.Context, nodeID, instanceID string) error {
	if nodeID == "" || instanceID == "" {
		return errors.New("sequence: node identity is required")
	}
	now, err := storageNowExpression(d.db.Name())
	if err != nil {
		return err
	}
	// The target row and the proposed row are both visible to an upsert. Use
	// the target table qualifier so PostgreSQL does not treat last_seen_at as
	// ambiguous inside ON CONFLICT DO UPDATE.
	ageColumn := "last_seen_at"
	if d.db.Name() != "sqlite" {
		ageColumn = "sequence_node_liveness.last_seen_at"
	}
	age, err := elapsedExpression(d.db.Name(), ageColumn)
	if err != nil {
		return err
	}
	query := "INSERT INTO sequence_node_liveness (node_id, instance_id, last_seen_at, eligible_since) VALUES (?, ?, " + now + ", " + now + ") ON CONFLICT (node_id) DO UPDATE SET eligible_since = CASE WHEN sequence_node_liveness.instance_id <> excluded.instance_id OR " + age + " >= ? THEN excluded.eligible_since ELSE sequence_node_liveness.eligible_since END, instance_id = excluded.instance_id, last_seen_at = excluded.last_seen_at"
	if d.db.Name() == "mysql" {
		query = "INSERT INTO sequence_node_liveness (node_id, instance_id, last_seen_at, eligible_since) VALUES (?, ?, " + now + ", " + now + ") ON DUPLICATE KEY UPDATE eligible_since = CASE WHEN instance_id <> VALUES(instance_id) OR " + age + " >= ? THEN VALUES(eligible_since) ELSE eligible_since END, instance_id = VALUES(instance_id), last_seen_at = VALUES(last_seen_at)"
	}
	return d.db.WithContext(ctx).Exec(query, nodeID, instanceID, d.ttl.Microseconds()).Error
}

func (d *livenessData) DropLiveness(ctx context.Context, nodeID, instanceID string) error {
	return d.db.WithContext(ctx).
		Exec("DELETE FROM sequence_node_liveness WHERE node_id = ? AND instance_id = ?", nodeID, instanceID).
		Error
}

// storageNowExpression returns the dialect's current-timestamp expression.
func storageNowExpression(dialect string) (string, error) {
	switch dialect {
	case "postgres":
		return "clock_timestamp()", nil
	case "mysql":
		return "CURRENT_TIMESTAMP(6)", nil
	case "sqlite":
		return "strftime('%Y-%m-%d %H:%M:%f', 'now')", nil
	default:
		return "", fmt.Errorf("sequence: unsupported dialect %q", dialect)
	}
}

func elapsedExpression(dialect, column string) (string, error) {
	switch dialect {
	case "postgres":
		return "CAST(EXTRACT(EPOCH FROM (clock_timestamp() - " + column + ")) * 1000000 AS bigint)", nil
	case "mysql":
		return "TIMESTAMPDIFF(MICROSECOND, " + column + ", CURRENT_TIMESTAMP(6))", nil
	case "sqlite":
		return "CAST((julianday('now') - julianday(" + column + ")) * 86400000000 AS integer)", nil
	default:
		return "", fmt.Errorf("sequence: unsupported dialect %q", dialect)
	}
}

// RouteModel is the single-row route snapshot persisted in sequence_route_snapshot.
type RouteModel struct {
	ID        int       `gorm:"column:id;primaryKey;autoIncrement:false"`
	Version   int64     `gorm:"column:version"`
	Payload   []byte    `gorm:"column:payload"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName returns the route snapshot table name.
func (RouteModel) TableName() string { return "sequence_route_snapshot" }

type routeData struct{ db *gorm.DB }

// NewRouteModel builds the route snapshot reader over db.
func NewRouteModel(db *gorm.DB) biz.RouteRepo { return &routeData{db: db} }

func (d *routeData) GetNewerRoute(ctx context.Context, version int64) (*biz.Route, error) {
	var model RouteModel
	err := d.db.WithContext(ctx).Where("id = 1 AND version > ?", version).Take(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return biz.DecodeRoute(model.Version, model.Payload)
}
