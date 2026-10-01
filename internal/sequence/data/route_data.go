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

// RouteModel stores a serialized sequence route in the owner database.
type RouteModel struct {
	Version   int64     `gorm:"column:version;primaryKey;autoIncrement:false"`
	Payload   []byte    `gorm:"column:payload;not null"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
}

// TableName returns the route table name.
func (RouteModel) TableName() string { return "sequence_routes" }

type routeData struct {
	db *gormio.DB
}

// NewRouteModel constructs the route repository backed by db.
func NewRouteModel(db *gormio.DB) biz.RouteRepo {
	return &routeData{db: db}
}

func (d *routeData) GetNewerRoute(ctx context.Context, version int64) (*biz.Route, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("route gorm store: database is required")
	}

	var model RouteModel
	err := d.db.WithContext(ctx).
		Where("version > ?", version).
		Order("version DESC").
		Take(&model).Error
	if errors.Is(err, gormio.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query route newer than %d: %w", version, err)
	}
	// The payload format is shared with the placement service, which writes it,
	// so the decoder is the one both processes agree on rather than a second copy
	// that only has to look alike.
	return biz.DecodeRoute(model.Version, model.Payload)
}

// AdvanceRouteRevision moves the directory revision forward by one and returns
// the new value.
//
// It runs inside the caller's transaction, so the advance and the insert of the
// route that carries it are one unit: a directory that failed to write never
// consumes a revision, and two publishers that both reached this point are
// serialised by the row lock the UPDATE takes.
//
// MySQL has no UPDATE ... RETURNING, so the new value is read back with a second
// statement in the same transaction. That read is safe precisely because the
// transaction still holds the row.
func AdvanceRouteRevision(
	ctx context.Context,
	tx *gormio.DB,
	dialect string,
) (int64, error) {
	if dialect == "postgres" {
		var state struct {
			Revision int64 `gorm:"column:revision"`
		}
		if err := tx.WithContext(ctx).Raw(
			"UPDATE sequence_route_state SET revision = revision + 1 WHERE id = 1 " +
				"RETURNING revision",
		).Scan(&state).Error; err != nil {
			return 0, fmt.Errorf("advance route revision: %w", err)
		}
		return state.Revision, nil
	}
	if err := tx.WithContext(ctx).Exec(
		"UPDATE sequence_route_state SET revision = revision + 1 WHERE id = 1",
	).Error; err != nil {
		return 0, fmt.Errorf("advance route revision: %w", err)
	}
	var state struct {
		Revision int64 `gorm:"column:revision"`
	}
	if err := tx.WithContext(ctx).Raw(
		"SELECT revision FROM sequence_route_state WHERE id = 1",
	).Scan(&state).Error; err != nil {
		return 0, fmt.Errorf("read route revision: %w", err)
	}
	return state.Revision, nil
}
