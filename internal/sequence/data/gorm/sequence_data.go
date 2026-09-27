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
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/codesjoy/sindri/internal/sequence/biz"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SequenceModel stores a reserved sequence range in the owner database.
type SequenceModel struct {
	SequenceKey string    `gorm:"column:sequence_key;size:256;primaryKey"`
	MaxID       int64     `gorm:"column:max_id;not null"`
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

func (d *sequenceData) ReserveRanges(
	ctx context.Context,
	requests []biz.ReservationRequest,
) ([]biz.SequenceRange, error) {
	if d == nil || d.db == nil {
		return nil, errors.New("sequence gorm store: database is required")
	}
	if err := validateReservationRequests(requests); err != nil {
		return nil, err
	}

	sorted := append([]biz.ReservationRequest(nil), requests...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	switch d.db.Name() {
	case "postgres":
		return d.reservePostgres(ctx, requests, sorted)
	case "mysql":
		return d.reserveTransactionalMySQL(ctx, requests, sorted, false)
	case "sqlite":
		return d.reserveTransactionalMySQL(ctx, requests, sorted, true)
	default:
		return nil, fmt.Errorf(
			"sequence gorm store: unsupported dialect %q",
			d.db.Name(),
		)
	}
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

func (d *sequenceData) reservePostgres(
	ctx context.Context,
	requests []biz.ReservationRequest,
	sorted []biz.ReservationRequest,
) ([]biz.SequenceRange, error) {
	query, args := batchInsertQuery(
		sorted,
		" ON CONFLICT (sequence_key) DO UPDATE SET "+
			"max_id = sequence_ranges.max_id + EXCLUDED.max_id, "+
			"updated_at = EXCLUDED.updated_at RETURNING sequence_key, max_id",
	)
	var rows []SequenceModel
	if err := d.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("reserve sequence ranges: %w", err)
	}
	return buildReservedRanges(requests, rows)
}

func (d *sequenceData) reserveTransactionalMySQL(
	ctx context.Context,
	requests []biz.ReservationRequest,
	sorted []biz.ReservationRequest,
	sqlite bool,
) ([]biz.SequenceRange, error) {
	var reserved []biz.SequenceRange
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		suffix := " ON DUPLICATE KEY UPDATE " +
			"max_id = sequence_ranges.max_id + VALUES(max_id), " +
			"updated_at = VALUES(updated_at)"
		if sqlite {
			suffix = " ON CONFLICT (sequence_key) DO UPDATE SET " +
				"max_id = sequence_ranges.max_id + excluded.max_id, " +
				"updated_at = excluded.updated_at"
		}
		query, args := batchInsertQuery(sorted, suffix)
		if err := tx.Exec(query, args...).Error; err != nil {
			return fmt.Errorf("advance sequence ranges: %w", err)
		}

		keys := make([]string, len(sorted))
		for index, request := range sorted {
			keys[index] = request.Key
		}
		var rows []SequenceModel
		dbQuery := tx.WithContext(ctx).Where("sequence_key IN ?", keys)
		if !sqlite {
			dbQuery = dbQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := dbQuery.Find(&rows).Error; err != nil {
			return fmt.Errorf("read reserved sequence ranges: %w", err)
		}
		var err error
		reserved, err = buildReservedRanges(requests, rows)
		return err
	})
	if err != nil {
		return nil, err
	}
	return reserved, nil
}

func batchInsertQuery(
	requests []biz.ReservationRequest,
	suffix string,
) (string, []any) {
	var query strings.Builder
	query.WriteString("INSERT INTO sequence_ranges (sequence_key, max_id, updated_at) VALUES ")
	args := make([]any, 0, len(requests)*3)
	now := time.Now().UTC()
	for index, request := range requests {
		if index > 0 {
			query.WriteString(", ")
		}
		query.WriteString("(?, ?, ?)")
		args = append(args, request.Key, request.Step, now)
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
		if row.MaxID < request.Step {
			return nil, fmt.Errorf(
				"reserve sequence %q: invalid maximum %d",
				request.Key,
				row.MaxID,
			)
		}
		reserved[index] = biz.SequenceRange{
			Start: row.MaxID - request.Step + 1,
			End:   row.MaxID,
		}
		if reserved[index].Start <= 0 {
			return nil, fmt.Errorf("reserve sequence %q: maximum overflow", request.Key)
		}
	}
	return reserved, nil
}
