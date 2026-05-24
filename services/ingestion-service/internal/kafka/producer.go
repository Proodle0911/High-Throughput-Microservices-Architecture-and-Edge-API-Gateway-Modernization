package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

type PaymentProducer interface {
	PublishPaymentEvent(ctx context.Context, msg interface{}) error
	Close() error
}

type paymentProducer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

func NewPaymentProducer(broker, topic string, logger *zap.Logger) PaymentProducer {
	return &paymentProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(broker),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
			// Production-ready settings
			RequiredAcks: kafka.RequireAll, // Wait for all replicas
			Async:        false,            // Synchronous write for high durability
			MaxAttempts:  3,
		},
		logger: logger,
	}
}

func (p *paymentProducer) PublishPaymentEvent(ctx context.Context, msg interface{}) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Prepare Kafka headers for OTel propagation
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	
	headers := make([]kafka.Header, 0)
	for k, v := range carrier {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Value:   payload,
		Headers: headers,
	})
	if err != nil {
		p.logger.Error("failed to publish message to kafka", zap.Error(err))
		return err
	}

	return nil
}

func (p *paymentProducer) Close() error {
	return p.writer.Close()
}
