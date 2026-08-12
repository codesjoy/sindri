package xgorm

import (
	"testing"
	"time"

	"github.com/codesjoy/pkg/basic/xgorm"
	"github.com/stretchr/testify/require"
)

func TestConfigPreservesOptionsAndPoolSettings(t *testing.T) {
	option := xgorm.WithMaxOpenConns(37)
	cfg := Config{
		DSN:              "postgres://example",
		ExpectedDatabase: "owner",
		ExpectedAccount:  "owner",
		MaxOpenConns:     20,
		MaxIdleConns:     5,
		ConnMaxLifetime:  30 * time.Minute,
		Options:          []xgorm.Option{option},
	}
	if len(cfg.Options) != 1 || cfg.MaxOpenConns != 20 || cfg.ConnMaxLifetime != 30*time.Minute {
		t.Fatalf("database config was not preserved: %#v", cfg)
	}
}

func TestConfigValidateRejectsInvalidValues(t *testing.T) {
	valid := Config{
		Driver:           DriverPostgres,
		DSN:              "postgres://example",
		ExpectedDatabase: "owner",
		ExpectedAccount:  "owner",
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Minute,
	}
	require.NoError(t, valid.Validate())

	tests := []Config{
		{Driver: "sqlite"},
		{Driver: DriverPostgres},
		{Driver: DriverPostgres, DSN: "dsn"},
		{
			Driver: DriverPostgres, DSN: "dsn", ExpectedDatabase: "owner",
			ExpectedAccount: "owner", MaxOpenConns: -1, MaxIdleConns: 1,
			ConnMaxLifetime: time.Minute,
		},
		{
			Driver: DriverPostgres, DSN: "dsn", ExpectedDatabase: "owner",
			ExpectedAccount: "owner", MaxOpenConns: 1, MaxIdleConns: 2,
			ConnMaxLifetime: time.Minute,
		},
	}
	for _, cfg := range tests {
		require.Error(t, cfg.Validate())
	}
}

func TestConfigDefaultsToPostgres(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	if cfg.Driver != DriverPostgres {
		t.Fatalf("driver = %q, want %q", cfg.Driver, DriverPostgres)
	}
	if cfg.MaxOpenConns != 20 || cfg.MaxIdleConns != 5 || cfg.ConnMaxLifetime != 30*time.Minute {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}

func TestDialectorForSupportedDrivers(t *testing.T) {
	tests := []struct {
		driver string
		want   string
	}{
		{driver: DriverPostgres, want: "postgres"},
		{driver: DriverMySQL, want: "mysql"},
	}
	for _, test := range tests {
		t.Run(test.driver, func(t *testing.T) {
			dialector, err := dialectorFor(test.driver, "ignored")
			if err != nil {
				t.Fatal(err)
			}
			if dialector.Name() != test.want {
				t.Fatalf("dialector = %q, want %q", dialector.Name(), test.want)
			}
		})
	}
	if _, err := dialectorFor("sqlite", "ignored"); err == nil {
		t.Fatal("expected unsupported driver error")
	}
}

func TestNilDatabaseCloseIsSafe(t *testing.T) {
	var db *Database
	if err := db.Close(); err != nil {
		t.Fatalf("nil close: %v", err)
	}
}
