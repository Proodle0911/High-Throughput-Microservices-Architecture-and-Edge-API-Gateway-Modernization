package consumer

import (
	"context"
	"encoding/json"

	"github.com/pranav/portfolio-arch/services/ledger-service/internal/db"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type PaymentEvent struct {
	TransactionID      string  `json:"transaction_id"`
	AccountID          string  `json:"account_id"`
	Amount             float64 `json:"amount"`
	Currency           string  `json:"currency"`
	DestinationAccount string  `json:"destination_account"`
}

type LedgerConsumer struct {
	reader *kafka.Reader
	repo   db.LedgerRepo
	logger *zap.Logger
}

func NewLedgerConsumer(broker, topic, groupID string, repo db.LedgerRepo, logger *zap.Logger) *LedgerConsumer {
	return &LedgerConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker},
			Topic:   topic,
			GroupID: groupID,
			// For high-throughput, we can tune these
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
		repo:   repo,
		logger: logger,
	}
}

func (c *LedgerConsumer) Start(ctx context.Context) error {
	c.logger.Info("Ledger consumer started...")
	tracer := otel.Tracer("ledger-consumer")

	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Error("failed to read message from kafka", zap.Error(err))
			continue
		}

		// Extract context from Kafka headers
		carrier := propagation.MapCarrier{}
		for _, h := range m.Headers {
			carrier[h.Key] = string(h.Value)
		}
		extractedCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

		// Start a new span for the processing
		childCtx, span := tracer.Start(extractedCtx, "process_payment_event", 
			trace.WithAttributes(
				attribute.String("transaction_id", string(m.Key)),
			),
		)

		var event PaymentEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			c.logger.Error("failed to unmarshal payment event", zap.Error(err))
			span.End()
			continue
		}

		span.SetAttributes(attribute.String("tx_id", event.TransactionID))

		c.logger.Info("Processing payment event", zap.String("tx_id", event.TransactionID))

		err = c.repo.ProcessTransaction(childCtx, event.TransactionID, event.AccountID, event.DestinationAccount, event.Amount, event.Currency)
		if err != nil {
			c.logger.Error("failed to process transaction in ledger", zap.Error(err))
			span.RecordError(err)
			span.End()
			continue
		}

		c.logger.Info("Transaction settled successfully", zap.String("tx_id", event.TransactionID))
		span.End()
	}
}


func (c *LedgerConsumer) Close() error {
	return c.reader.Close()
}
