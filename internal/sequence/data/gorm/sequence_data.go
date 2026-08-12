package gorm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/codesjoy/skuld/internal/sequence/biz"
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

func (d *sequenceData) ReserveRange(
	ctx context.Context,
	key string,
	step int64,
) (biz.SequenceRange, error) {
	if d == nil || d.db == nil {
		return biz.SequenceRange{}, errors.New("sequence gorm store: database is required")
	}
	if strings.TrimSpace(key) == "" || len(key) > 256 {
		return biz.SequenceRange{}, errors.New("sequence gorm store: key must contain 1..256 bytes")
	}
	if step <= 0 {
		return biz.SequenceRange{}, errors.New("sequence gorm store: step must be positive")
	}

	var reserved biz.SequenceRange
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()
		model := SequenceModel{SequenceKey: key, MaxID: step, UpdatedAt: now}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "sequence_key"}},
			DoUpdates: clause.Assignments(map[string]any{
				"max_id": gorm.Expr(
					"? + ?",
					clause.Column{Table: clause.CurrentTable, Name: "max_id"},
					step,
				),
				"updated_at": now,
			}),
		}).Create(&model).Error; err != nil {
			return fmt.Errorf("advance sequence %q: %w", key, err)
		}

		var current SequenceModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("sequence_key = ?", key).
			Take(&current).Error; err != nil {
			return fmt.Errorf("read reserved sequence %q: %w", key, err)
		}
		if current.MaxID < step {
			return fmt.Errorf("reserve sequence %q: invalid maximum %d", key, current.MaxID)
		}
		reserved = biz.SequenceRange{
			Start: current.MaxID - step + 1,
			End:   current.MaxID,
		}
		if reserved.Start <= 0 {
			return fmt.Errorf("reserve sequence %q: maximum overflow", key)
		}
		return nil
	})
	if err != nil {
		return biz.SequenceRange{}, err
	}
	return reserved, nil
}
