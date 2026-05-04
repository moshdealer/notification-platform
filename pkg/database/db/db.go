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

// Connect — только подключение (используется везде)
func Connect(dbCfg *config.DatabaseCfg) (*gorm.DB, error) {
	dsn := dbCfg.DatabaseDSN
	if dsn == "" {
		return nil, fmt.Errorf("POSTGRES_DSN env is not set")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// Настройки пула соединений
	sqlDB.SetMaxIdleConns(15)
	sqlDB.SetMaxOpenConns(60)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	sqlDB.SetConnMaxIdleTime(10 * time.Minute)

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DB: %w", err)
	}
	
	log.Println("Successfully connected to PostgreSQL")
	return db, nil
}

// Migrate — использует отдельное соединение, чтобы не ломать основной пул
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
