# High-Throughput Microservices Architecture - Project Mandates

## Core Principles
- **Performance First:** All code must be optimized for low latency and high throughput. Use gRPC/HTTP2 for internal communication and Redpanda for async event streaming.
- **Resilience:** Implement Circuit Breakers, Retries with Exponential Backoff, and Dead Letter Queues (DLQ).
- **Data Integrity:** Use the **Transactional Outbox Pattern** for all database-to-event-bus transitions to ensure "exactly-once" processing semantics where possible, or "at-least-once" with idempotency.
- **Observability:** No service is production-ready without OpenTelemetry tracing, Prometheus metrics, and structured JSON logging.

## Tech Stack
- **Edge:** Envoy Proxy (L7 Load Balancer, JWT Auth, Rate Limiting).
- **Services:** Go (Primary) for ingestion/processing; Java/Spring Boot (Secondary) for complex business logic if needed.
- **State:** PostgreSQL 16 (Relational), Redis 7 (Caching/Rate Limiting).
- **Messaging:** Redpanda (Kafka-API compatible).
- **Infrastructure:** Docker Compose (Local), Kubernetes (Target).

## Phase 1 Checklist
- [x] Directory Structure initialized.
- [x] Core Infrastructure (Envoy, Redpanda, Postgres, Redis) configured in `docker-compose.yml`.
- [x] Baseline Envoy configuration for health checks.
- [x] Phase 2: Ingestion Service Implementation (Go/gRPC).
- [x] Phase 3: Ledger Service Implementation (Asynchronous Processing).
- [x] Phase 4: Observability & Distributed Tracing (OTel/Jaeger).
- [ ] Phase 5: Performance Benchmarking.
