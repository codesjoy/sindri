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

package gorm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	"gorm.io/gorm"
)

// RouteModel stores a serialized sequence route in the owner database.
type RouteModel struct {
	Version   int64     `gorm:"column:version;primaryKey;autoIncrement:false"`
	Payload   []byte    `gorm:"column:payload;not null"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
}

// TableName returns the route table name.
func (RouteModel) TableName() string { return "sequence_routes" }

type routePayload struct {
	Nodes []storedRouteNode `json:"nodes"`
}

type storedRouteNode struct {
	NodeID string   `json:"node_id"`
	Slots  []uint32 `json:"slots"`
}

type routeData struct {
	db *gorm.DB
}

// NewRouteModel constructs the route repository backed by db.
func NewRouteModel(db *gorm.DB) biz.RouteRepo {
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
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query route newer than %d: %w", version, err)
	}
	return decodeRoute(model)
}

func decodeRoute(model RouteModel) (*biz.Route, error) {
	if model.Version <= 0 {
		return nil, fmt.Errorf("decode route: invalid version %d", model.Version)
	}
	var payload routePayload
	if err := json.Unmarshal(model.Payload, &payload); err != nil {
		return nil, fmt.Errorf("decode route version %d: %w", model.Version, err)
	}

	nodes := make([]biz.RouteNode, 0, len(payload.Nodes))
	nodeIDs := make(map[string]struct{}, len(payload.Nodes))
	assigned := make([]bool, biz.SlotCount)
	assignedCount := 0
	for _, stored := range payload.Nodes {
		if stored.NodeID == "" {
			return nil, fmt.Errorf("decode route version %d: empty node id", model.Version)
		}
		if _, exists := nodeIDs[stored.NodeID]; exists {
			return nil, fmt.Errorf(
				"decode route version %d: duplicate node %q",
				model.Version,
				stored.NodeID,
			)
		}
		nodeIDs[stored.NodeID] = struct{}{}

		slots := append([]uint32(nil), stored.Slots...)
		for _, slot := range slots {
			if slot >= biz.SlotCount {
				return nil, fmt.Errorf(
					"decode route version %d: slot %d is out of range",
					model.Version,
					slot,
				)
			}
			slotIndex := int(slot)
			if assigned[slotIndex] {
				return nil, fmt.Errorf(
					"decode route version %d: slot %d is assigned more than once",
					model.Version,
					slot,
				)
			}
			assigned[slotIndex] = true
			assignedCount++
		}
		sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
		nodes = append(nodes, biz.RouteNode{NodeID: stored.NodeID, Slots: slots})
	}
	if assignedCount != biz.SlotCount {
		return nil, fmt.Errorf(
			"decode route version %d: assigned %d of %d slots",
			model.Version,
			assignedCount,
			biz.SlotCount,
		)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	return &biz.Route{Version: model.Version, Nodes: nodes}, nil
}
