# Architecture Review: CQRS, Event-Driven, Saga & Fault Tolerance

**Review Date:** 2026-05-17  
**Reviewer:** Bob (AI Software Engineer)  
**System:** POS Transaction Processing System

---

## Executive Summary

This document reviews the current implementation against four key architectural patterns:
1. **CQRS (Command Query Responsibility Segregation)**
2. **Event-Driven Architecture**
3. **Saga Pattern**
4. **Fault Tolerance Practices**

### Overall Assessment
- ✅ **Strengths:** Basic event-driven flow, dual database setup, error handling with DLQ
- ⚠️ **Concerns:** Incomplete CQRS separation, missing saga orchestration, limited fault tolerance
- 🔴 **Critical Issues:** No compensation logic, weak consistency guarantees, missing idempotency

---

## 1. CQRS (Command Query Responsibility Segregation)

### Current Implementation

#### ✅ What's Working
1. **Dual Database Setup**
   - MongoDB for write-side (pending transactions)
   - PostgreSQL for read-side (completed transactions)
   - Clear separation of concerns

2. **Write Model (Commands)**
   ```java
   // TransactionService.java
   - createTransaction() → writes to MongoDB
   - selectOffer() → writes to both MongoDB & PostgreSQL
   ```

3. **Read Model (Queries)**
   ```java
   // TransactionController.java
   - getOffers() → reads from MongoDB
   - getFinalTransaction() → reads from PostgreSQL
   ```

#### ❌ Issues & Violations

1. **Mixed Responsibilities in TransactionService**
   - Service handles both commands AND queries
   - Violates single responsibility principle
   ```java
   // PROBLEM: Same service does both
   public String createTransaction() { } // Command
   public Object getOffers() { }         // Query
   ```

2. **No Command/Query Objects**
   - Missing explicit Command pattern
   - No CommandHandler/QueryHandler separation
   - DTOs used directly instead of commands

3. **Inconsistent Data Models**
   - `PendingTransaction` and `TransactionEntity` have different fields
   - No clear event sourcing or projection strategy

4. **Direct Repository Access in Controller**
   ```java
   // TransactionController.java line 28
   private final TransactionRepository transactionRepository;
   ```
   - Controller bypasses service layer for queries
   - Breaks CQRS query path

### 🎯 Recommendations

#### 1. Separate Command and Query Services
```java
// Command Side
@Service
public class TransactionCommandService {
    public String handleCreateTransaction(CreateTransactionCommand cmd);
    public void handleSelectOffer(SelectOfferCommand cmd);
}

// Query Side
@Service
public class TransactionQueryService {
    public TransactionDTO getTransaction(String id);
    public List<OfferDTO> getOffers(String transactionId);
}
```

#### 2. Implement Command/Query Objects
```java
// Commands
public record CreateTransactionCommand(String userId, Double amount) {}
public record SelectOfferCommand(String transactionId, String offerId) {}

// Queries
public record GetTransactionQuery(String transactionId) {}
public record GetOffersQuery(String transactionId) {}
```

#### 3. Add Command/Query Handlers
```java
@Component
public class CreateTransactionCommandHandler {
    public String handle(CreateTransactionCommand command) {
        // Validation, business logic, persistence
    }
}

@Component
public class GetTransactionQueryHandler {
    public TransactionDTO handle(GetTransactionQuery query) {
        // Read from optimized read model
    }
}
```

#### 4. Implement Read Model Projections
```java
@Service
public class TransactionProjectionService {
    @EventListener
    public void on(TransactionCreatedEvent event) {
        // Update read model
    }
    
    @EventListener
    public void on(OfferSelectedEvent event) {
        // Update read model
    }
}
```

---

## 2. Event-Driven Architecture

### Current Implementation

#### ✅ What's Working

1. **Kafka Integration**
   - Producer/Consumer setup
   - Event serialization with JSON
   - Topic-based messaging

2. **Event Publishing**
   ```java
   // TransactionService.java lines 51-58
   TransactionEvent event = TransactionEvent.builder()
       .transactionId(transactionId)
       .userId(request.getUserId())
       .amount(request.getAmount())
       .status(status)
       .build();
   producer.sendTransactionEvent(event);
   ```

3. **Asynchronous Processing**
   - Consumer processes events independently
   - Decoupled transaction creation from offer generation

#### ❌ Issues & Violations

1. **Limited Event Types**
   - Only one event: `TransactionEvent`
   - Missing domain events:
     - `TransactionCreatedEvent`
     - `OffersGeneratedEvent`
     - `OfferSelectedEvent`
     - `TransactionCompletedEvent`
     - `TransactionCancelledEvent`

2. **No Event Versioning**
   ```java
   // TransactionEvent.java - Missing version field
   public class TransactionEvent {
       // No eventVersion or eventType field
   }
   ```

3. **Synchronous Database Writes Before Event**
   ```java
   // TransactionService.java lines 38-47
   pendingRepo.save(txn);  // Synchronous write
   producer.sendTransactionEvent(event);  // Then event
   ```
   - Risk: Database succeeds but Kafka fails
   - No transactional outbox pattern

4. **Missing Event Metadata**
   - No timestamp, correlation ID, causation ID
   - No event source tracking
   - No event ordering guarantees

5. **Mutable Event Status**
   ```java
   // TransactionConsumer.java line 28
   event.setStatus("FAILED");  // Mutating event!
   ```
   - Events should be immutable

6. **No Event Store**
   - Events not persisted for replay
   - No audit trail
   - Cannot rebuild state from events

#### 🎯 Recommendations

#### 1. Define Proper Domain Events
```java
// Base Event
@Data
@SuperBuilder
public abstract class DomainEvent {
    private String eventId;
    private String eventType;
    private int eventVersion;
    private Instant timestamp;
    private String correlationId;
    private String causationId;
}

// Specific Events
public class TransactionCreatedEvent extends DomainEvent {
    private String transactionId;
    private String userId;
    private Double amount;
}

public class OffersGeneratedEvent extends DomainEvent {
    private String transactionId;
    private List<Offer> offers;
}

public class OfferSelectedEvent extends DomainEvent {
    private String transactionId;
    private String offerId;
}
```

#### 2. Implement Transactional Outbox Pattern
```java
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {
    @Id
    private String id;
    private String aggregateId;
    private String eventType;
    private String payload;
    private Instant createdAt;
    private boolean published;
}

@Service
public class OutboxService {
    @Transactional
    public void saveAndPublish(DomainEvent event) {
        // 1. Save to outbox table
        outboxRepository.save(toOutboxEvent(event));
        // 2. Background job publishes to Kafka
    }
}
```

#### 3. Add Event Store
```java
@Document(collection = "event_store")
public class StoredEvent {
    @Id
    private String id;
    private String aggregateId;
    private String eventType;
    private int version;
    private String payload;
    private Instant timestamp;
}

@Service
public class EventStore {
    public void append(DomainEvent event);
    public List<DomainEvent> getEvents(String aggregateId);
}
```

#### 4. Implement Event Versioning
```java
public interface EventUpgrader {
    DomainEvent upgrade(String payload, int fromVersion, int toVersion);
}

@Component
public class TransactionEventUpgrader implements EventUpgrader {
    public DomainEvent upgrade(String payload, int from, int to) {
        // Handle version migrations
    }
}
```

---

## 3. Saga Pattern

### Current Implementation

#### ✅ What's Working

1. **Multi-Step Process**
   - Create transaction → Generate offers → Select offer → Complete
   - State transitions tracked in MongoDB

2. **Timeout Handling**
   ```java
   // TransactionTimeoutService.java
   @Scheduled(fixedRate = 10000)
   public void cancelTimedOutTransactions()
   ```

#### ❌ Critical Issues

1. **NO SAGA ORCHESTRATION**
   - No saga coordinator
   - No saga state machine
   - No saga persistence

2. **NO COMPENSATION LOGIC**
   ```java
   // TransactionService.java line 97
   transactionRepository.save(entity);  // What if this fails?
   pendingRepo.deleteById(transactionId);  // Already committed!
   ```
   - No rollback mechanism
   - No compensating transactions
   - Data inconsistency risk

3. **Missing Saga States**
   - No explicit saga lifecycle
   - No saga execution tracking
   - No saga recovery mechanism

4. **Distributed Transaction Issues**
   ```java
   // selectOffer() method
   txn.setStatus("PROCESSING");
   pendingRepo.save(txn);           // MongoDB write
   transactionRepository.save(entity);  // PostgreSQL write
   pendingRepo.deleteById(transactionId);  // MongoDB delete
   ```
   - Three separate database operations
   - No atomicity guarantee
   - Partial failure scenarios not handled

5. **No Idempotency**
   - Consumer can process same event multiple times
   - No deduplication mechanism
   - Risk of duplicate transactions

#### 🎯 Recommendations

#### 1. Implement Saga Orchestrator
```java
@Entity
@Table(name = "saga_instances")
public class SagaInstance {
    @Id
    private String sagaId;
    private String transactionId;
    private String currentStep;
    private String status; // STARTED, COMPENSATING, COMPLETED, FAILED
    private String payload;
    private Instant createdAt;
    private Instant updatedAt;
}

@Service
public class TransactionSaga {
    
    public void start(CreateTransactionCommand command) {
        SagaInstance saga = createSaga(command);
        executeStep(saga, "CREATE_TRANSACTION");
    }
    
    private void executeStep(SagaInstance saga, String step) {
        try {
            switch(step) {
                case "CREATE_TRANSACTION":
                    createTransaction(saga);
                    executeStep(saga, "GENERATE_OFFERS");
                    break;
                case "GENERATE_OFFERS":
                    generateOffers(saga);
                    executeStep(saga, "WAIT_FOR_SELECTION");
                    break;
                case "COMPLETE_TRANSACTION":
                    completeTransaction(saga);
                    saga.setStatus("COMPLETED");
                    break;
            }
        } catch (Exception e) {
            compensate(saga, step);
        }
    }
    
    private void compensate(SagaInstance saga, String failedStep) {
        saga.setStatus("COMPENSATING");
        switch(failedStep) {
            case "COMPLETE_TRANSACTION":
                compensateCompleteTransaction(saga);
                // Fall through
            case "GENERATE_OFFERS":
                compensateGenerateOffers(saga);
                // Fall through
            case "CREATE_TRANSACTION":
                compensateCreateTransaction(saga);
                break;
        }
        saga.setStatus("FAILED");
    }
}
```

#### 2. Add Compensation Methods
```java
@Service
public class TransactionCompensationService {
    
    public void compensateCreateTransaction(String transactionId) {
        // Delete from MongoDB
        pendingRepo.deleteById(transactionId);
        // Publish TransactionCancelledEvent
    }
    
    public void compensateGenerateOffers(String transactionId) {
        // Clear offers from transaction
        PendingTransaction txn = pendingRepo.findById(transactionId).get();
        txn.setOffers(null);
        txn.setStatus("PENDING");
        pendingRepo.save(txn);
    }
    
    public void compensateCompleteTransaction(String transactionId) {
        // Delete from PostgreSQL
        transactionRepository.deleteById(transactionId);
        // Restore in MongoDB
        // Publish CompensationCompletedEvent
    }
}
```

#### 3. Implement Idempotency
```java
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {
    @Id
    private String eventId;
    private String transactionId;
    private Instant processedAt;
}

@Service
public class IdempotentEventProcessor {
    
    @Transactional
    public void processIfNotProcessed(TransactionEvent event, 
                                     Consumer<TransactionEvent> processor) {
        if (processedEventRepo.existsById(event.getEventId())) {
            log.info("Event {} already processed, skipping", event.getEventId());
            return;
        }
        
        processor.accept(event);
        
        processedEventRepo.save(new ProcessedEvent(
            event.getEventId(),
            event.getTransactionId(),
            Instant.now()
        ));
    }
}
```

#### 4. Add Saga State Machine
```java
public enum SagaStep {
    CREATE_TRANSACTION("GENERATE_OFFERS", "COMPENSATE_CREATE"),
    GENERATE_OFFERS("WAIT_FOR_SELECTION", "COMPENSATE_OFFERS"),
    WAIT_FOR_SELECTION("COMPLETE_TRANSACTION", "COMPENSATE_SELECTION"),
    COMPLETE_TRANSACTION(null, "COMPENSATE_COMPLETE");
    
    private final String nextStep;
    private final String compensationStep;
    
    // Constructor and methods
}

@Service
public class SagaStateMachine {
    public SagaStep getNextStep(SagaStep current);
    public SagaStep getCompensationStep(SagaStep failed);
    public boolean canTransition(SagaStep from, SagaStep to);
}
```

---

## 4. Fault Tolerance Practices

### Current Implementation

#### ✅ What's Working

1. **Kafka Error Handling**
   ```java
   // KafkaConfig.java lines 20-26
   DeadLetterPublishingRecoverer recoverer = 
       new DeadLetterPublishingRecoverer(kafkaTemplate,
           (record, ex) -> new TopicPartition("transactions-dlq", record.partition()));
   return new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3));
   ```
   - 3 retry attempts with 2-second backoff
   - DLQ for failed messages

2. **Exception Handling in Consumer**
   ```java
   // TransactionConsumer.java lines 22-34
   try {
       processingService.process(event);
   } catch (Exception ex) {
       log.error("Processing failed", ex);
       throw ex; // Triggers DLQ
   }
   ```

3. **Timeout Service**
   - Scheduled cleanup of stale transactions
   - Prevents indefinite pending state

#### ❌ Critical Issues

1. **No Circuit Breaker**
   - No protection against cascading failures
   - External service calls (Drools) not protected
   - No fallback mechanisms

2. **No Retry Strategy for Database Operations**
   ```java
   // TransactionService.java line 97
   transactionRepository.save(entity);  // No retry if fails
   ```

3. **Missing Health Checks**
   - No readiness/liveness probes
   - No dependency health monitoring
   - No graceful degradation

4. **No Rate Limiting**
   - No protection against traffic spikes
   - No backpressure handling
   - Can overwhelm Kafka/databases

5. **Insufficient Monitoring**
   - No metrics collection
   - No distributed tracing
   - No alerting on failures

6. **No Bulkhead Pattern**
   - All operations share same thread pool
   - One slow operation can block others

7. **Weak Consistency Guarantees**
   ```java
   // TransactionProcessingService.java line 48
   pendingRepo.deleteById(txn.getTransactionId());
   throw e; // Already deleted!
   ```
   - Delete happens before exception propagates
   - Inconsistent state

8. **No Saga Recovery**
   - Failed sagas not tracked
   - No automatic retry of failed sagas
   - Manual intervention required

#### 🎯 Recommendations

#### 1. Add Circuit Breaker (Resilience4j)
```xml
<!-- pom.xml -->
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-spring-boot3</artifactId>
    <version>2.1.0</version>
</dependency>
```

```java
@Service
public class OfferService {
    
    @CircuitBreaker(name = "drools", fallbackMethod = "generateDefaultOffers")
    @Retry(name = "drools", fallbackMethod = "generateDefaultOffers")
    @Bulkhead(name = "drools")
    public List<Offer> generateOffers(String userId, Double amount) {
        // Existing logic
    }
    
    private List<Offer> generateDefaultOffers(String userId, Double amount, 
                                              Exception ex) {
        log.warn("Drools unavailable, using defaults", ex);
        return List.of(createDefaultOffer(amount));
    }
}
```

```yaml
# application.yml
resilience4j:
  circuitbreaker:
    instances:
      drools:
        slidingWindowSize: 10
        failureRateThreshold: 50
        waitDurationInOpenState: 10s
        permittedNumberOfCallsInHalfOpenState: 3
  retry:
    instances:
      drools:
        maxAttempts: 3
        waitDuration: 1s
        exponentialBackoffMultiplier: 2
  bulkhead:
    instances:
      drools:
        maxConcurrentCalls: 10
        maxWaitDuration: 100ms
```

#### 2. Implement Database Retry Logic
```java
@Service
public class ResilientTransactionService {
    
    @Retryable(
        value = {DataAccessException.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public void saveTransaction(TransactionEntity entity) {
        transactionRepository.save(entity);
    }
    
    @Recover
    public void recoverSaveTransaction(DataAccessException ex, 
                                      TransactionEntity entity) {
        log.error("Failed to save transaction after retries", ex);
        // Publish failure event for saga compensation
        eventPublisher.publish(new TransactionSaveFailedEvent(entity));
    }
}
```

#### 3. Add Health Checks
```java
@Component
public class KafkaHealthIndicator implements HealthIndicator {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Override
    public Health health() {
        try {
            // Check Kafka connectivity
            kafkaTemplate.send("health-check", "ping").get(5, TimeUnit.SECONDS);
            return Health.up().withDetail("kafka", "available").build();
        } catch (Exception e) {
            return Health.down()
                .withDetail("kafka", "unavailable")
                .withException(e)
                .build();
        }
    }
}

@Component
public class MongoHealthIndicator implements HealthIndicator {
    
    private final MongoTemplate mongoTemplate;
    
    @Override
    public Health health() {
        try {
            mongoTemplate.executeCommand("{ ping: 1 }");
            return Health.up().withDetail("mongodb", "available").build();
        } catch (Exception e) {
            return Health.down()
                .withDetail("mongodb", "unavailable")
                .withException(e)
                .build();
        }
    }
}
```

#### 4. Implement Rate Limiting
```java
@Component
public class RateLimitingInterceptor implements HandlerInterceptor {
    
    private final RateLimiter rateLimiter = RateLimiter.create(100.0); // 100 req/sec
    
    @Override
    public boolean preHandle(HttpServletRequest request, 
                            HttpServletResponse response, 
                            Object handler) {
        if (!rateLimiter.tryAcquire()) {
            response.setStatus(429); // Too Many Requests
            return false;
        }
        return true;
    }
}
```

#### 5. Add Distributed Tracing
```xml
<!-- pom.xml -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-brave</artifactId>
</dependency>
<dependency>
    <groupId>io.zipkin.reporter2</groupId>
    <artifactId>zipkin-reporter-brave</artifactId>
</dependency>
```

```java
@Configuration
public class TracingConfig {
    
    @Bean
    public Tracer tracer() {
        return Tracing.newBuilder()
            .localServiceName("pos-transaction-service")
            .spanReporter(zipkinSpanReporter())
            .build()
            .tracer();
    }
}
```

#### 6. Implement Saga Recovery Service
```java
@Service
public class SagaRecoveryService {
    
    @Scheduled(fixedRate = 60000) // Every minute
    public void recoverFailedSagas() {
        List<SagaInstance> failedSagas = sagaRepository
            .findByStatusAndUpdatedAtBefore(
                "COMPENSATING", 
                Instant.now().minus(5, ChronoUnit.MINUTES)
            );
        
        for (SagaInstance saga : failedSagas) {
            log.info("Recovering saga {}", saga.getSagaId());
            try {
                sagaOrchestrator.resume(saga);
            } catch (Exception e) {
                log.error("Failed to recover saga {}", saga.getSagaId(), e);
                // Alert operations team
            }
        }
    }
}
```

#### 7. Add Metrics and Monitoring
```java
@Service
public class MetricsService {
    
    private final MeterRegistry meterRegistry;
    
    public void recordTransactionCreated() {
        meterRegistry.counter("transactions.created").increment();
    }
    
    public void recordOfferGeneration(long durationMs) {
        meterRegistry.timer("offers.generation.time")
            .record(durationMs, TimeUnit.MILLISECONDS);
    }
    
    public void recordSagaFailure(String step) {
        meterRegistry.counter("saga.failures", "step", step).increment();
    }
}
```

---

## 5. Additional Recommendations

### 5.1 Data Consistency

#### Implement Eventual Consistency Checks
```java
@Service
public class ConsistencyCheckService {
    
    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void checkConsistency() {
        // Find transactions in Postgres but not in Mongo (completed)
        // Find transactions in Mongo older than timeout (orphaned)
        // Reconcile discrepancies
    }
}
```

### 5.2 Security

#### Add Authentication & Authorization
```java
@Configuration
@EnableWebSecurity
public class SecurityConfig {
    
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) {
        return http
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/v1/transactions/**").authenticated()
                .anyRequest().permitAll()
            )
            .oauth2ResourceServer(OAuth2ResourceServerConfigurer::jwt)
            .build();
    }
}
```

### 5.3 API Versioning

#### Implement Proper Versioning
```java
@RestController
@RequestMapping("/api/v2/transactions")
public class TransactionControllerV2 {
    // New version with breaking changes
}
```

### 5.4 Documentation

#### Add OpenAPI Documentation
```java
@Configuration
public class OpenApiConfig {
    
    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
            .info(new Info()
                .title("POS Transaction API")
                .version("1.0")
                .description("Transaction processing with offer negotiation"));
    }
}
```

---

## 6. Priority Action Items

### 🔴 Critical (Implement Immediately)

1. **Add Saga Compensation Logic**
   - Implement compensating transactions
   - Add saga state persistence
   - Handle partial failures

2. **Implement Idempotency**
   - Add event deduplication
   - Track processed events
   - Prevent duplicate transactions

3. **Fix Data Consistency Issues**
   - Use transactional outbox pattern
   - Add consistency checks
   - Implement proper error handling

### 🟡 High Priority (Next Sprint)

4. **Separate CQRS Concerns**
   - Split command and query services
   - Implement command/query handlers
   - Add read model projections

5. **Add Circuit Breakers**
   - Protect external service calls
   - Implement fallback mechanisms
   - Add bulkhead pattern

6. **Implement Proper Event Types**
   - Define domain events
   - Add event versioning
   - Create event store

### 🟢 Medium Priority (Future)

7. **Add Distributed Tracing**
   - Implement correlation IDs
   - Add Zipkin/Jaeger integration
   - Create trace dashboards

8. **Enhance Monitoring**
   - Add metrics collection
   - Create alerting rules
   - Build operational dashboards

9. **Implement Rate Limiting**
   - Add API rate limits
   - Implement backpressure
   - Add quota management

---

## 7. Conclusion

The current implementation has a solid foundation with event-driven architecture and dual database setup. However, it requires significant improvements in:

1. **CQRS Implementation** - Needs proper separation of commands and queries
2. **Saga Pattern** - Missing orchestration and compensation logic
3. **Fault Tolerance** - Lacks circuit breakers, retries, and recovery mechanisms
4. **Data Consistency** - Vulnerable to partial failures and inconsistent states

### Estimated Effort
- Critical fixes: 2-3 weeks
- High priority items: 3-4 weeks
- Medium priority items: 4-6 weeks

### Risk Assessment
- **Current Risk Level:** HIGH
- **Risk After Critical Fixes:** MEDIUM
- **Risk After All Fixes:** LOW

---

**Document Version:** 1.0  
**Last Updated:** 2026-05-17  
**Next Review:** After implementing critical fixes