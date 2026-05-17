# Fraud Detection System - Backend Engineer Onboarding Documentation

## Table of Contents
1. [High-Level Architecture](#1-high-level-architecture)
2. [Kafka Producers and Consumers](#2-kafka-producers-and-consumers)
3. [CQRS Boundaries](#3-cqrs-boundaries)
4. [MongoDB vs PostgreSQL Responsibilities](#4-mongodb-vs-postgresql-responsibilities)
5. [Transaction Lifecycle](#5-transaction-lifecycle)
6. [Failure Handling](#6-failure-handling)
7. [DLQ Behavior](#7-dlq-behavior)
8. [Saga-Like Workflows](#8-saga-like-workflows)
9. [Development Setup](#development-setup)
10. [API Reference](#api-reference)
11. [Monitoring & Observability](#monitoring--observability)
12. [Common Pitfalls & Solutions](#common-pitfalls--solutions)

---

## 1. High-Level Architecture

This is a **POS (Point of Sale) Transaction Processing System** with fraud detection and offer negotiation capabilities, built using event-driven architecture with CQRS pattern.

### Technology Stack
- **Framework**: Spring Boot 4.0.5 (Java 17)
- **Message Broker**: Apache Kafka
- **Databases**: MongoDB (Write/Temporary), PostgreSQL (Read/Permanent)
- **Rules Engine**: Drools 8.44.0
- **Build Tool**: Maven

### Architecture Flow

```
Client → TransactionController → TransactionService
                                       ↓
                                   MongoDB (PENDING)
                                       ↓
                                   Kafka Topic
                                       ↓
                              TransactionConsumer
                                       ↓
                         TransactionProcessingService
                                       ↓
                          OfferService (Drools Rules)
                                       ↓
                              MongoDB (PROCESSING)
                                       ↓
                    User Selects Offer → PostgreSQL (COMPLETED)
                                       ↓
                              MongoDB (Deleted)

Parallel: TransactionTimeoutService (60s) → PostgreSQL (CANCELLED)
Error Path: Consumer Error → Retry (3x) → DLQ
```

---

## 2. Kafka Producers and Consumers

### Producer: `TransactionProducer`
**Location**: `src/main/java/com/pos/kafka/producer/TransactionProducer.java`

**Purpose**: Publishes high-value transaction events (amount > 50,000) to Kafka.

```java
public void sendTransactionEvent(TransactionEvent event) {
    kafkaTemplate.send("transactions", event.getTransactionId(), event);
}
```

**Configuration**:
- Topic: `transactions`
- Key: `transactionId` (ensures ordering)
- Serialization: JSON

### Consumer: `TransactionConsumer`
**Location**: `src/main/java/com/pos/kafka/consumer/TransactionConsumer.java`

**Purpose**: Consumes transaction events and triggers offer generation.

```java
@KafkaListener(topics = "transactions", groupId = "pos-group")
public void consume(TransactionEvent event) {
    try {
        processingService.process(event);
    } catch (Exception ex) {
        throw ex; // Triggers DLQ
    }
}
```

**Configuration**:
- Consumer Group: `pos-group`
- Auto-offset Reset: `earliest`
- Error Handling: Throws exception for DLQ

---

## 3. CQRS Boundaries

### Write Side - MongoDB
**Entity**: `PendingTransaction`
- Collection: `pending_transactions`
- Purpose: Temporary operational data
- Lifecycle: Created → Updated → Deleted

**States**: PENDING, PROCESSING

### Read Side - PostgreSQL
**Entity**: `TransactionEntity`
- Table: `transactions`
- Purpose: Permanent audit trail
- Lifecycle: Created only at terminal state

**Terminal States**: COMPLETED, CANCELLED

---

## 4. MongoDB vs PostgreSQL Responsibilities

### MongoDB (Operational Store)
- **Use Cases**: Temporary state, high write throughput, flexible schema
- **Data**: Active transactions with embedded offers array
- **Queries**: TTL-based timeout detection
- **Cleanup**: Deleted after completion/cancellation

### PostgreSQL (System of Record)
- **Use Cases**: Permanent storage, ACID compliance, reporting
- **Data**: Immutable transaction history
- **Queries**: SQL analytics and business intelligence
- **Retention**: Never deleted (audit trail)

---

## 5. Transaction Lifecycle

### State Machine
```
[START] → PENDING (MongoDB)
           ↓
       amount > 50k? → Kafka Event
           ↓
       PROCESSING (MongoDB + Offers)
           ↓
       ├─ User selects (< 60s) → COMPLETED (PostgreSQL)
       ├─ Timeout (> 60s) → CANCELLED (PostgreSQL)
       └─ Error → FAILED → DLQ
```

### Phase 1: Creation
`POST /api/v1/transactions` → Generate UUID → Save to MongoDB → Publish to Kafka (if high-value)

### Phase 2: Processing
Kafka Consumer → Load user data → Execute Drools rules → Generate offers → Update MongoDB

### Phase 3: User Action
`GET /offers` → User reviews → `POST /select-offer` → Save to PostgreSQL → Delete from MongoDB

### Phase 4: Timeout
Scheduler (every 10s) → Find stale transactions (> 60s) → Save as CANCELLED → Delete from MongoDB

---

## 6. Failure Handling

### Error Strategy
```
Processing Error
    ↓
Consumer catches exception
    ↓
Delete from MongoDB (cleanup)
    ↓
Throw exception
    ↓
Kafka Error Handler
    ↓
Retry 3 times (2s backoff)
    ↓
Send to DLQ
```

### Key Components

**1. Consumer Exception Handling**
```java
catch (Exception ex) {
    log.error("Processing failed", ex);
    throw ex; // Triggers DLQ
}
```

**2. Processing Cleanup**
```java
catch (Exception e) {
    pendingRepo.deleteById(txn.getTransactionId()); // Prevent orphans
    throw e;
}
```

**3. Kafka Error Handler**
```java
new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3));
// 3 retries, 2 seconds apart
```

---

## 7. DLQ Behavior

### Configuration
- **DLQ Topic**: `transactions-dlq`
- **Retry Policy**: 3 attempts, 2s backoff
- **Total Time**: ~6 seconds before DLQ

### When Messages Go to DLQ
1. Drools rules engine failure
2. MongoDB connection issues
3. Data validation errors
4. Unhandled runtime exceptions

### DLQ Message Structure
```json
{
  "originalTopic": "transactions",
  "key": "transaction-id",
  "value": { "transactionId": "...", "status": "FAILED" },
  "exception": { "class": "...", "message": "..." }
}
```

### Recommended DLQ Handler
```java
@KafkaListener(topics = "transactions-dlq", groupId = "dlq-handler")
public void handleDLQ(TransactionEvent event) {
    // 1. Log to monitoring
    // 2. Send alert
    // 3. Store in error database
    // 4. Manual retry after fix
}
```

---

## 8. Saga-Like Workflows

### Compensating Transaction Pattern

**Happy Path**:
```
Create → Process → Complete → Cleanup
```

**Failure Path**:
```
Create → Process → Error → Delete (Compensate) → DLQ
```

### Saga Characteristics

**1. Compensating Actions**: MongoDB deletion on failure

**2. State Transitions**:
- Success: PENDING → PROCESSING → COMPLETED
- Timeout: PENDING → PROCESSING → CANCELLED
- Error: PENDING → FAILED → DLQ

**3. Eventual Consistency**: MongoDB and PostgreSQL eventually consistent

**4. Timeout Coordinator**: `TransactionTimeoutService` acts as saga coordinator

### Idempotency
```java
if (!"PENDING".equals(txn.getStatus())) return; // Prevent duplicate processing
```

---

## Development Setup

### Prerequisites
```bash
java -version  # Java 17
mvn -version   # Maven 3.8+
docker --version  # Docker 20+
```

### Infrastructure (Docker Compose)
```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    ports: ["2181:2181"]
  
  kafka:
    image: confluentinc/cp-kafka:7.4.0
    ports: ["9092:9092"]
  
  mongodb:
    image: mongo:6.0
    ports: ["27017:27017"]
  
  postgres:
    image: postgres:15
    ports: ["5432:5432"]
    environment:
      POSTGRES_DB: testdb
      POSTGRES_PASSWORD: postgres
```

### Run Application
```bash
# Start infrastructure
docker-compose up -d

# Set environment
export DB_PASSWORD=postgres

# Run application
mvn spring-boot:run

# Access Swagger
http://localhost:8080/swagger-ui.html
```

### Testing Flow
```bash
# 1. Create transaction
curl -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -d '{"userId": "user123", "amount": 75000}'

# 2. Wait 2-3 seconds

# 3. Get offers
curl http://localhost:8080/api/v1/transactions/{id}/offers

# 4. Select offer
curl -X POST http://localhost:8080/api/v1/transactions/{id}/select-offer \
  -H "Content-Type: application/json" \
  -d '{"offerId": "offer-uuid"}'

# 5. Verify
curl http://localhost:8080/api/v1/transactions/{id}
```

---

## API Reference

### Endpoints

**1. Create Transaction**
```
POST /api/v1/transactions
Body: {"userId": "string", "amount": number}
Response: "transaction-id"
```

**2. Get Offers**
```
GET /api/v1/transactions/{id}/offers
Response: [{"offerId": "...", "type": "...", "description": "..."}]
```

**3. Select Offer**
```
POST /api/v1/transactions/{id}/select-offer
Body: {"offerId": "string"}
Response: "Offer selected"
```

**4. Get Transaction**
```
GET /api/v1/transactions/{id}
Response: {"transactionId": "...", "status": "...", ...}
```

**5. Get All Transactions**
```
GET /api/v1/transactions/all
Response: [{"transactionId": "...", ...}]
```

---

## Monitoring & Observability

### Key Metrics
1. **Kafka Lag**: Consumer group lag on `transactions` topic
2. **DLQ Rate**: Messages per minute in `transactions-dlq`
3. **MongoDB Query Time**: Timeout query performance
4. **PostgreSQL Write Latency**: Transaction finalization time
5. **Timeout Rate**: Percentage of cancelled transactions

### Log Patterns
```
INFO  - Sending transaction event
INFO  - Received transaction event
INFO  - Rules fired: 3
ERROR - Processing failed for txn
WARN  - Auto-cancelled txn
```

---

## Common Pitfalls & Solutions

### 1. Orphaned MongoDB Records
**Problem**: Failed transactions remain in MongoDB  
**Solution**: Delete on error in `TransactionProcessingService`

### 2. Duplicate Processing
**Problem**: Kafka redelivery causes duplicates  
**Solution**: Status check before processing (idempotency)

### 3. DLQ Accumulation
**Problem**: No DLQ consumer, messages pile up  
**Solution**: Implement DLQ handler with monitoring

### 4. Timeout Race Condition
**Problem**: User selects while scheduler cancels  
**Solution**: Status validation in `selectOffer()`

### 5. Drools Rules Not Firing
**Problem**: Empty offers despite meeting conditions  
**Solution**: Verify `kmodule.xml` configuration

---

## Code Structure

```
src/main/java/com/pos/
├── HackathonApplication.java
├── controller/TransactionController.java
├── service/
│   ├── TransactionService.java
│   ├── TransactionProcessingService.java
│   ├── TransactionTimeoutService.java
│   └── OfferService.java
├── kafka/
│   ├── producer/TransactionProducer.java
│   ├── consumer/TransactionConsumer.java
│   └── KafkaConfig.java
├── entity/
│   ├── PendingTransaction.java (MongoDB)
│   └── TransactionEntity.java (PostgreSQL)
└── repository/
    ├── PendingTransactionRepository.java
    └── TransactionRepository.java
```

---

## Next Steps for New Engineers

**Week 1**: Read code, run locally, trace transactions  
**Week 2**: Modify Drools rules, add logging, write tests  
**Week 3**: Implement DLQ handler, add metrics, optimize queries  
**Week 4**: Load testing, monitoring setup, create runbook

---

## Resources

- [Spring Boot Docs](https://docs.spring.io/spring-boot/)
- [Spring Kafka Docs](https://docs.spring.io/spring-kafka/)
- [MongoDB Docs](https://docs.mongodb.com/)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [Drools Docs](https://docs.drools.org/)

---

**Questions?** Contact the team lead or check the project wiki.