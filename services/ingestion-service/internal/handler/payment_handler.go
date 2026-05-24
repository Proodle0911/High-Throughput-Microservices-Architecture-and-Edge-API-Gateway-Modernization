package handler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pranav/portfolio-arch/proto/payment"
	"github.com/pranav/portfolio-arch/services/ingestion-service/internal/kafka"
	"github.com/pranav/portfolio-arch/services/ingestion-service/internal/repository"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PaymentHandler struct {
	paymentv1.UnimplementedPaymentIngressServiceServer
	idempotencyRepo repository.IdempotencyRepo
	producer        kafka.PaymentProducer
	logger          *zap.Logger
}

func NewPaymentHandler(repo repository.IdempotencyRepo, prod kafka.PaymentProducer, logger *zap.Logger) *PaymentHandler {
	return &PaymentHandler{
		idempotencyRepo: repo,
		producer:        prod,
		logger:          logger,
	}
}

func (h *PaymentHandler) ProcessPayment(ctx context.Context, req *paymentv1.ProcessPaymentRequest) (*paymentv1.ProcessPaymentResponse, error) {
	ctx, span := otel.Tracer("ingestion-handler").Start(ctx, "ProcessPayment")
	defer span.End()

	span.SetAttributes(
		attribute.String("idempotency_key", req.IdempotencyKey),
		attribute.String("account_id", req.AccountId),
	)

	// 1. Basic Validation
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	if req.Amount <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be positive")
	}

	// 2. Idempotency Check (Redis)
	// We use a 24-hour expiration for idempotency keys
	ok, err := h.idempotencyRepo.SetIfNotExist(ctx, req.IdempotencyKey, 24*time.Hour)
	if err != nil {
		h.logger.Error("idempotency check failed", zap.Error(err))
		return nil, status.Error(codes.Internal, "internal server error")
	}
	if !ok {
		return nil, status.Error(codes.AlreadyExists, "duplicate request")
	}

	// 3. Generate Transaction ID
	transactionID := uuid.New().String()

	// 4. Publish to Kafka (Redpanda)
	event := map[string]interface{}{
		"transaction_id":      transactionID,
		"account_id":          req.AccountId,
		"amount":              req.Amount,
		"currency":            req.Currency,
		"destination_account": req.DestinationAccount,
		"status":              "VALIDATED",
		"timestamp":           time.Now().Unix(),
	}

	err = h.producer.PublishPaymentEvent(ctx, event)
	if err != nil {
		// In a production system, we might want to implement a fallback or 
		// compensate for the idempotency lock if the publish fails.
		// For now, we log and return error.
		h.logger.Error("failed to publish payment event", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to process transaction")
	}

	h.logger.Info("payment processed successfully", 
		zap.String("transaction_id", transactionID),
		zap.String("idempotency_key", req.IdempotencyKey))

	return &paymentv1.ProcessPaymentResponse{
		TransactionId: transactionID,
		Status:        paymentv1.PaymentStatus_PAYMENT_STATUS_VALIDATED,
		Message:       "Transaction accepted and queued",
	}, nil
}
