package main

import (
	"context"
	"database/sql"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/pranav/portfolio-arch/internal/otel"
	"github.com/pranav/portfolio-arch/services/ledger-service/internal/consumer"
	"github.com/pranav/portfolio-arch/services/ledger-service/internal/db"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	logger.Info("Starting Ledger Service...")

	// 0. Initialize OpenTelemetry
	tp, err := otel.InitTracer(context.Background(), "ledger-service", "localhost:4317")
	if err != nil {
		sugar.Fatalw("failed to initialize tracer", "error", err)
	}
	defer tp.Shutdown(context.Background())

	// 1. Initialize Postgres Connection
	// connStr := "postgresql://admin:password@localhost:5432/portfolio_db?sslmode=disable"
	connStr := "host=localhost port=5432 user=admin password=password dbname=portfolio_db sslmode=disable"
	database, err := sql.Open("postgres", connStr)
	if err != nil {
		logger.Fatal("failed to connect to postgres", zap.Error(err))
	}
	defer database.Close()

	if err := database.Ping(); err != nil {
		logger.Fatal("failed to ping postgres", zap.Error(err))
	}

	// 2. Initialize Repository and Consumer
	ledgerRepo := db.NewPostgresLedgerRepo(database, logger)
	ledgerConsumer := consumer.NewLedgerConsumer("localhost:19092", "payment-events", "ledger-group", ledgerRepo, logger)
	defer ledgerConsumer.Close()

	// 3. Start Consuming
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := ledgerConsumer.Start(ctx); err != nil {
			logger.Error("consumer stopped with error", zap.Error(err))
		}
	}()

	// 4. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down Ledger Service...")
	cancel()
}
