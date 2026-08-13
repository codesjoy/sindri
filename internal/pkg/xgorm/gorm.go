// Package xgorm contains the shared GORM foundation used by owner services.
package xgorm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	gormtx "github.com/codesjoy/pkg/basic/transaction/gorm"
	"github.com/codesjoy/pkg/basic/xgorm"
	"github.com/codesjoy/yggdrasil/v3"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	gormio "gorm.io/gorm"
)

const (
	// DriverPostgres identifies the PostgreSQL database driver.
	DriverPostgres = "postgres"
	// DriverMySQL identifies the MySQL database driver.
	DriverMySQL = "mysql"
)

// Config is the process-local database configuration contract. Expected
// identity values remain service-owned validation inputs, while the common
// connection and instrumentation options live here.
type Config struct {
	Driver           string         `mapstructure:"driver"`
	DSN              string         `mapstructure:"dsn"`
	ExpectedDatabase string         `mapstructure:"expected_database"`
	ExpectedAccount  string         `mapstructure:"expected_account"`
	MaxOpenConns     int            `mapstructure:"max_open_conns"`
	MaxIdleConns     int            `mapstructure:"max_idle_conns"`
	ConnMaxLifetime  time.Duration  `mapstructure:"conn_max_lifetime"`
	Options          []xgorm.Option `mapstructure:"-"`
}

// SetDefaults applies the shared connection-pool defaults.
func (c *Config) SetDefaults() {
	if strings.TrimSpace(c.Driver) == "" {
		c.Driver = DriverPostgres
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 20
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 5
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = 30 * time.Minute
	}
}

// Validate checks the complete database connection contract before opening it.
func (c Config) Validate() error {
	driver := strings.TrimSpace(c.Driver)
	if driver != DriverPostgres && driver != DriverMySQL {
		return fmt.Errorf("shared database: unsupported driver %q", driver)
	}
	if strings.TrimSpace(c.DSN) == "" {
		return errors.New("shared database: dsn is required")
	}
	if strings.TrimSpace(c.ExpectedDatabase) == "" || strings.TrimSpace(c.ExpectedAccount) == "" {
		return errors.New("shared database: expected database and account are required")
	}
	if c.MaxOpenConns <= 0 {
		return errors.New("shared database: max open connections must be positive")
	}
	if c.MaxIdleConns < 0 || c.MaxIdleConns > c.MaxOpenConns {
		return errors.New(
			"shared database: max idle connections must be within 0..max open connections",
		)
	}
	if c.ConnMaxLifetime <= 0 {
		return errors.New("shared database: connection max lifetime must be positive")
	}
	return nil
}

// Database is the shared owner database handle and transaction runner.
type Database struct {
	DB *gormio.DB
	Tx *gormtx.Runner
}

// New opens and verifies an owner database using runtime observability
// providers. The identity check prevents a workload from silently connecting
// to another owner's logical database.
func New(rt yggdrasil.Runtime, cfg Config) (*Database, error) {
	if rt == nil {
		return nil, errors.New("shared database: runtime is required")
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	driver := strings.TrimSpace(cfg.Driver)
	dialector, err := dialectorFor(driver, cfg.DSN)
	if err != nil {
		return nil, err
	}
	logger := rt.Logger()
	var tracer trace.Tracer
	if rt.TracerProvider() != nil {
		tracer = rt.TracerProvider().Tracer("github.com/codesjoy/sindri/shared/database")
	}
	var meter metric.Meter
	if rt.MeterProvider() != nil {
		meter = rt.MeterProvider().Meter("github.com/codesjoy/sindri/shared/database")
	}

	opts := append([]xgorm.Option(nil), cfg.Options...)
	opts = append(opts, xgorm.WithSlogLogger(logger))
	if tracer != nil {
		opts = append(opts, xgorm.WithTracer(tracer))
	}
	if meter != nil {
		opts = append(opts, xgorm.WithMeter(meter))
	}
	if cfg.MaxOpenConns > 0 {
		opts = append(opts, xgorm.WithMaxOpenConns(cfg.MaxOpenConns))
	}
	if cfg.MaxIdleConns > 0 {
		opts = append(opts, xgorm.WithMaxIdleConns(cfg.MaxIdleConns))
	}
	if cfg.ConnMaxLifetime > 0 {
		opts = append(opts, xgorm.WithConnMaxLifetime(int(cfg.ConnMaxLifetime.Seconds())))
	}

	db, err := xgorm.New(dialector, opts...)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", driver, err)
	}
	databaseName, account, err := resolveIdentity(context.Background(), db, driver)
	if err != nil {
		_ = closeDatabase(db)
		return nil, fmt.Errorf("resolve database identity: %w", err)
	}
	if databaseName != cfg.ExpectedDatabase || account != cfg.ExpectedAccount {
		_ = closeDatabase(db)
		return nil, fmt.Errorf(
			"database identity is %s/%s, expected %s/%s",
			databaseName,
			account,
			cfg.ExpectedDatabase,
			cfg.ExpectedAccount,
		)
	}
	return &Database{DB: db, Tx: gormtx.New(db)}, nil
}

func dialectorFor(driver, dsn string) (gormio.Dialector, error) {
	switch driver {
	case DriverPostgres:
		return postgres.Open(dsn), nil
	case DriverMySQL:
		return mysql.Open(dsn), nil
	default:
		return nil, fmt.Errorf("shared database: unsupported driver %q", driver)
	}
}

func resolveIdentity(ctx context.Context, db *gormio.DB, driver string) (string, string, error) {
	var query string
	switch driver {
	case DriverPostgres:
		query = "SELECT current_database(), current_user"
	case DriverMySQL:
		query = "SELECT DATABASE(), SUBSTRING_INDEX(CURRENT_USER(), '@', 1)"
	default:
		return "", "", fmt.Errorf("unsupported driver %q", driver)
	}
	var databaseName, account string
	if err := db.WithContext(ctx).Raw(query).Row().Scan(&databaseName, &account); err != nil {
		return "", "", err
	}
	return databaseName, account, nil
}

// Close releases the database connection and instrumentation resources.
func (d *Database) Close() error {
	if d == nil {
		return nil
	}
	return closeDatabase(d.DB)
}

func closeDatabase(db *gormio.DB) error {
	if db == nil {
		return nil
	}
	_ = xgorm.CloseMetrics(db)
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
