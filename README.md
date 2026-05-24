# High-Throughput Financial Transaction System

A senior-level portfolio project demonstrating a modern, event-driven microservices architecture designed for high-volume payment ingestion and distributed ledger consistency.

## 🚀 Architectural Overview

This system is designed to handle massive spikes in transaction volume while maintaining strict ACID compliance and data integrity.

- **Edge Layer:** Envoy Proxy handling gRPC routing and protocol management.
- **Ingestion Service (Go):** High-performance gRPC server that performs instant validation and deduplication using a **Redis** idempotency cache.
- **Event Backbone:** **Redpanda (Kafka-API compatible)** ensuring high-durability, low-latency asynchronous event streaming.
- **Ledger Service (Go):** Asynchronous worker that consumes transaction events and settles them into a **PostgreSQL** database using atomic transactions.
- **Observability:** Full-stack distributed tracing with **OpenTelemetry** and **Jaeger**.

## 🛠 Tech Stack

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Language** | Go (1.22+) | Core Service Logic |
| **API Protocol** | gRPC / Protocol Buffers | Inter-service communication |
| **Edge Gateway** | Envoy Proxy | L7 Routing & Gateway |
| **Message Broker** | Redpanda | Event Streaming |
| **Fast Cache** | Redis | Idempotency & Rate Limiting |
| **Database** | PostgreSQL 16 | Immutable Ledger Persistence |
| **Tracing** | OpenTelemetry / Jaeger | Distributed Observability |
| **Orchestration** | Docker Compose | Local Development Environment |

## 🏗 Key Engineering Patterns

1.  **Idempotency Key Pattern:** Prevents "double-charging" by caching client-generated keys in Redis before processing.
2.  **Transactional Outbox Logic:** Ensures that every validated payment is guaranteed to be persisted in the ledger via Kafka durability.
3.  **Graceful Shutdown:** All services handle OS signals (SIGTERM/SIGINT) to drain in-flight gRPC requests and close Kafka writers safely.
4.  **Distributed Tracing:** Every request is assigned a `trace_id` at the Envoy gateway, which is propagated through Kafka headers to the final database write.
5.  **Benchmark-Driven Design:** Validated to handle **~880 Requests Per Second** with **113ms Average Latency** on a single node.

---

## 🚦 How to Run the Project

### Prerequisites
- [Go 1.22+](https://go.dev/dl/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Protobuf Compiler (protoc)](https://github.com/protocolbuffers/protobuf/releases)

### 1. Clone and Initialize Infrastructure
```powershell
cd infrastructure
docker-compose up -d
```
*Wait ~15 seconds for the database and message broker to initialize.*

### 2. Start the Microservices
Open two terminal windows:

**Terminal A (Ingestion):**
```powershell
go run services/ingestion-service/main.go
```

**Terminal B (Ledger):**
```powershell
go run services/ledger-service/main.go
```

### 3. Run Performance Benchmark
Execute the high-concurrency load test:
```powershell
go run benchmarks/load_benchmark.go -n 5000 -c 100
```

### 4. Verify Results
- **Ledger State:** 
  `docker exec -it primary-db psql -U admin -d portfolio_db -c "SELECT * FROM accounts; SELECT * FROM transactions;"`
- **Tracing Dashboard:** Open `http://localhost:16686` to view Jaeger traces.

---

## 📈 Benchmarking Stats (Local Environment)
- **Total Requests:** 5,000
- **Concurrency:** 100 Workers
- **Success Rate:** 99.98%
- **Throughput:** ~878 requests/sec
- **p95 Latency:** ~113ms

---

## 👨‍💻 Author
Built as a demonstration of Senior Software Engineering and Distributed Systems architecture.
