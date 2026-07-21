package db

import (
	"fmt"
	"log"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepg "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/moshdealer/notification-platform/pkg/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Connect - только подключение (используется везде)
func Connect(dbCfg *config.DatabaseCfg) (*gorm.DB, error) {
	if dbCfg.DatabaseDSN == "" {
		return nil, fmt.Errorf("POSTGRES_DSN env is not set")
	}
	if dbCfg.MaxOpenConns <= 0 {
		return nil, fmt.Errorf(
			"database.max_open_conns must be greater than zero: %d",
			dbCfg.MaxOpenConns,
		)
	}
	if dbCfg.MaxIdleConns < 0 {
		return nil, fmt.Errorf(
			"database.max_idle_conns must not be negative: %d",
			dbCfg.MaxIdleConns,
		)
	}
	if dbCfg.MaxIdleConns > dbCfg.MaxOpenConns {
		return nil, fmt.Errorf(
			"database.max_idle_conns (%d) must not exceed max_open_conns (%d)",
			dbCfg.MaxIdleConns,
			dbCfg.MaxOpenConns,
		)
	}
	if dbCfg.ConnMaxLifetime <= 0 {
		return nil, fmt.Errorf(
			"database.conn_max_lifetime must be greater than zero: %s",
			dbCfg.ConnMaxLifetime,
		)
	}

	gormDB, err := gorm.Open(
		postgres.Open(dbCfg.DatabaseDSN),
		&gorm.Config{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}

	sqlDB, err := gormDB.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(dbCfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(dbCfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(dbCfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(10 * time.Minute)

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("failed to ping DB: %w", err)
	}

	log.Printf(
		"Successfully connected to PostgreSQL: max_open=%d max_idle=%d max_lifetime=%s",
		dbCfg.MaxOpenConns,
		dbCfg.MaxIdleConns,
		dbCfg.ConnMaxLifetime,
	)

	return gormDB, nil
}

// Migrate - использует отдельное соединение, чтобы не ломать основной пул
func Migrate(dbCfg *config.DatabaseCfg) error {
	migrateConn, err := Connect(dbCfg)
	if err != nil {
		return fmt.Errorf("failed to connect for migration: %w", err)
	}

	sqlDB, err := migrateConn.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB for migration: %w", err)
	}

	driver, err := migratepg.WithInstance(sqlDB, &migratepg.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migrate driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance("file://migrations", "postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}
	defer m.Close()

	log.Println("Running database migrations...")
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migration failed: %w", err)
	}

	log.Println("Migrations applied successfully (or no changes)")
	return nil
}
