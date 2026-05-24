package db

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

type LedgerRepo interface {
	ProcessTransaction(ctx context.Context, txID string, from, to string, amount float64, currency string) error
}

type postgresLedgerRepo struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewPostgresLedgerRepo(db *sql.DB, logger *zap.Logger) LedgerRepo {
	return &postgresLedgerRepo{db: db, logger: logger}
}

func (r *postgresLedgerRepo) ProcessTransaction(ctx context.Context, txID string, from, to string, amount float64, currency string) error {
	ctx, span := otel.Tracer("ledger-repo").Start(ctx, "db_process_transaction")
	defer span.End()

	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("tx_id", txID),
	)

	// Start a database transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Idempotency Check & Transaction Log
	// If the transaction_id already exists, we do nothing (Idempotent)
	query := `INSERT INTO transactions (transaction_id, from_account, to_account, amount, currency, status) 
              VALUES ($1, $2, $3, $4, $5, 'SETTLED') 
              ON CONFLICT (transaction_id) DO NOTHING`
	result, err := tx.ExecContext(ctx, query, txID, from, to, amount, currency)
	if err != nil {
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		r.logger.Info("Transaction already processed, skipping", zap.String("transaction_id", txID))
		return nil
	}

	// 2. Update Source Account (Subtract)
	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - $1, updated_at = NOW() WHERE account_id = $2", amount, from)
	if err != nil {
		return fmt.Errorf("failed to debit account %s: %w", from, err)
	}

	// 3. Update Destination Account (Add)
	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + $1, updated_at = NOW() WHERE account_id = $2", amount, to)
	if err != nil {
		return fmt.Errorf("failed to credit account %s: %w", to, err)
	}

	// Commit the transaction
	return tx.Commit()
}
