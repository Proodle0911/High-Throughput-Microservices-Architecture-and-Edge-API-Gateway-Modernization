package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pranav/portfolio-arch/internal/otel"
	"github.com/pranav/portfolio-arch/proto/payment"
	"github.com/pranav/portfolio-arch/services/ingestion-service/internal/handler"
	"github.com/pranav/portfolio-arch/services/ingestion-service/internal/kafka"
	"github.com/pranav/portfolio-arch/services/ingestion-service/internal/repository"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// 1. Initialize Structured Logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	sugar.Info("Starting Ingestion Service...")

	// 0. Initialize OpenTelemetry
	tp, err := otel.InitTracer(context.Background(), "ingestion-service", "localhost:4317")
	if err != nil {
		sugar.Fatalw("failed to initialize tracer", "error", err)
	}
	defer tp.Shutdown(context.Background())

	// 2. Initialize Redis for Idempotency
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Use env var in prod
	})
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		sugar.Fatalw("Failed to connect to Redis", "error", err)
	}
	idempotencyRepo := repository.NewRedisIdempotencyRepo(redisClient)

	// 3. Initialize Kafka Producer
	kafkaProducer := kafka.NewPaymentProducer("localhost:19092", "payment-events", logger)
	defer kafkaProducer.Close()

	// 4. Initialize gRPC Server
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		sugar.Fatalw("Failed to listen", "error", err)
	}

	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	paymentHandler := handler.NewPaymentHandler(idempotencyRepo, kafkaProducer, logger)
	paymentv1.RegisterPaymentIngressServiceServer(s, paymentHandler)

	// Register reflection service on gRPC server for debugging
	reflection.Register(s)

	// 5. Graceful Shutdown Handling
	go func() {
		sugar.Infof("gRPC server listening on %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			sugar.Fatalw("Failed to serve", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	sugar.Info("Shutting down gRPC server gracefully...")
	
	// GracefulStop stops the server from accepting new connections and finishes in-flight requests
	stopped := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		sugar.Info("Server stopped")
	case <-time.After(10 * time.Second):
		sugar.Warn("Forcing server shutdown after timeout")
		s.Stop()
	}
}
