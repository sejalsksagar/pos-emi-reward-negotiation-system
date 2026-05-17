# Transaction Trace: High-Value POS Transaction Flow

This document provides a detailed step-by-step trace of what happens when a POS transaction above the threshold (>50,000) is created, including all success and failure scenarios.

---

## Table of Contents
1. [Happy Path: Complete Success Flow](#happy-path-complete-success-flow)
2. [Failure Scenario 1: Kafka Publishing Failure](#failure-scenario-1-kafka-publishing-failure)
3. [Failure Scenario 2: Consumer Processing Failure](#failure-scenario-2-consumer-processing-failure)
4. [Failure Scenario 3: Drools Rules Engine Failure](#failure-scenario-3-drools-rules-engine-failure)
5. [Failure Scenario 4: Invalid Offer Selection](#failure-scenario-4-invalid-offer-selection)
6. [Failure Scenario 5: Timeout Cancellation](#failure-scenario-5-timeout-cancellation)
7. [Data Flow Summary](#data-flow-summary)

---

## Happy Path: Complete Success Flow

### Step 1: Client Request

**HTTP Request**:
```http
POST /api/v1/transactions HTTP/1.1
Host: localhost:8080
Content-Type: application/json

{
  "userId": "user123",
  "amount": 75000.0
}
```

**Entry Point**: `TransactionController.createTransaction()`
```java
@PostMapping
public ResponseEntity<String> createTransaction(@RequestBody CreateTransactionRequest request) {
    String transactionId = transactionService.createTransaction(request);
    return ResponseEntity.ok(transactionId);
}
```

---

### Step 2: Validation & Business Logic

**Location**: `TransactionService.createTransaction()`

**Code Flow**:
```java
public String createTransaction(CreateTransactionRequest request) {
    // 1. Generate unique transaction ID
    String transactionId = UUID.randomUUID().toString();
    // Result: "550e8400-e29b-41d4-a716-446655440000"
    
    // 2. Check threshold
    String status = request.getAmount() > THRESHOLD ? "PENDING" : "COMPLETED";
    // 75000 > 50000 → status = "PENDING"
    
    // Continue to Step 3...
}
```

**Validation**:
- ✅ Amount: 75000.0 (valid double)
- ✅ UserId: "user123" (not null)
- ✅ Threshold check: 75000 > 50000 → High-value transaction

---

### Step 3: MongoDB Write

**Location**: `TransactionService.createTransaction()` (continued)

**Code Flow**:
```java
// 3. Create pending transaction entity
PendingTransaction txn = PendingTransaction.builder()
    .transactionId(transactionId)
    .userId(request.getUserId())
    .amount(request.getAmount())
    .status(status)  // "PENDING"
    .createdAt(Instant.now())
    .offers(null)  // No offers yet
    .build();

// 4. Save to MongoDB
pendingRepo.save(txn);
```

**MongoDB Document Created**:
```json
{
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "user123",
  "amount": 75000.0,
  "status": "PENDING",
  "createdAt": ISODate("2026-05-17T04:00:00.000Z"),
  "offers": null,
  "_class": "com.pos.entity.PendingTransaction"
}
```

**Database State**:
- ✅ MongoDB: 1 document in `pending_transactions` collection
- ❌ PostgreSQL: No record yet

---

### Step 4: Kafka Publish

**Location**: `TransactionService.createTransaction()` (continued)

**Code Flow**:
```java
// 5. Check if high-value transaction
if ("PENDING".equals(status)) {
    // 6. Create Kafka event
    TransactionEvent event = TransactionEvent.builder()
        .transactionId(transactionId)
        .userId(request.getUserId())
        .amount(request.getAmount())
        .status(status)
        .build();
    
    // 7. Publish to Kafka
    producer.sendTransactionEvent(event);
}

// 8. Return transaction ID to client
return transactionId;
```

**Kafka Producer**: `TransactionProducer.sendTransactionEvent()`
```java
public void sendTransactionEvent(TransactionEvent event) {
    log.info("Sending transaction event: {}", event);
    // Topic: "transactions"
    // Key: "550e8400-e29b-41d4-a716-446655440000"
    // Value: TransactionEvent (JSON serialized)
    kafkaTemplate.send(topic, event.getTransactionId(), event);
}
```

**Kafka Message**:
```json
{
  "topic": "transactions",
  "partition": 0,
  "offset": 12345,
  "key": "550e8400-e29b-41d4-a716-446655440000",
  "value": {
    "transactionId": "550e8400-e29b-41d4-a716-446655440000",
    "userId": "user123",
    "amount": 75000.0,
    "status": "PENDING"
  },
  "timestamp": 1715918640000
}
```

**HTTP Response**:
```http
HTTP/1.1 200 OK
Content-Type: text/plain

550e8400-e29b-41d4-a716-446655440000
```

**System State at This Point**:
- ✅ Client received transaction ID
- ✅ MongoDB has PENDING transaction
- ✅ Kafka has message in queue
- ⏳ Async processing will happen next

---

### Step 5: Kafka Consumer

**Location**: `TransactionConsumer.consume()`

**Trigger**: Kafka listener automatically invokes when message arrives

**Code Flow**:
```java
@KafkaListener(topics = "${kafka.topic.transactions}", groupId = "pos-group")
public void consume(TransactionEvent event) {
    try {
        log.info("Received transaction event: {}", event);
        // Log: "Received transaction event: TransactionEvent(transactionId=550e8400...)"
        
        // Delegate to processing service
        processingService.process(event);
        
    } catch (Exception ex) {
        event.setStatus("FAILED");
        log.error("Processing failed for txn {}", event.getTransactionId(), ex);
        throw ex; // Important for DLQ
    }
}
```

**Consumer Group State**:
- Consumer: `pos-group`
- Partition: 0
- Offset: 12345 (committed after successful processing)

---

### Step 6: Transaction Processing Service

**Location**: `TransactionProcessingService.process()`

**Code Flow**:
```java
public void process(TransactionEvent event) {
    // 1. Fetch from MongoDB
    PendingTransaction txn = pendingRepo.findById(event.getTransactionId())
        .orElse(null);
    
    if (txn == null) return;  // Transaction not found
    
    // 2. Validate status
    if (!"PENDING".equals(txn.getStatus())) return;  // Already processed
    
    try {
        // 3. Generate offers (see Step 7)
        List<Offer> offers = offerService.generateOffers(
            event.getUserId(),
            event.getAmount()
        );
        
        // 4. Update transaction with offers
        txn.setOffers(offers);
        txn.setStatus("PROCESSING");
        
        // 5. Save back to MongoDB
        pendingRepo.save(txn);
        
    } catch (Exception e) {
        log.error("Error processing txn {}", txn.getTransactionId(), e);
        
        // CRITICAL: Remove from Mongo to prevent orphans
        pendingRepo.deleteById(txn.getTransactionId());
        
        throw e; // Sends to DLQ
    }
}
```

---

### Step 7: Rule Engine & Offer Generation

**Location**: `OfferService.generateOffers()`

**Code Flow**:
```java
public List<Offer> generateOffers(String userId, Double amount) {
    // 1. Load user data from CSV dataset
    Map<String, String> data = datasetService.getUserData(userId);
    
    // 2. Extract user attributes
    int creditScore = Integer.parseInt(data.getOrDefault("creditScore", "650"));
    String category = data.getOrDefault("merchantCategory", "general").toLowerCase();
    int rewardPoints = Integer.parseInt(data.getOrDefault("rewardPoints", "0"));
    
    // Example values for user123:
    // creditScore = 750
    // category = "electronics"
    // rewardPoints = 1200
    
    // 3. Create rule input
    RuleInput input = new RuleInput();
    input.setAmount(amount);           // 75000.0
    input.setCreditScore(creditScore); // 750
    input.setMerchantCategory(category); // "electronics"
    input.setRewardPoints(rewardPoints); // 1200
    
    // 4. Create Drools session
    KieSession kieSession = kieContainer.newKieSession("rulesSession");
    
    // 5. Insert facts into working memory
    kieSession.insert(input);
    
    // 6. Fire all rules
    int fired = kieSession.fireAllRules();
    System.out.println("Rules fired: " + fired);
    // Output: "Rules fired: 3"
    
    // 7. Dispose session
    kieSession.dispose();
    
    // 8. Return generated offers
    return input.getOffers();
}
```

**Drools Rules Evaluation** (`offer-rules.drl`):

**Rule 1: "Good Credit Score"**
```drools
rule "Good Credit Score"
when
    $input : RuleInput()
    eval($input.getCreditScore() > 700)  // 750 > 700 ✅
then
    $input.addOffer(new Offer(
        UUID.randomUUID().toString(),
        "EMI",
        "No-cost EMI (high credit score)"
    ));
end
```
**Result**: ✅ Offer added

**Rule 2: "Electronics Cashback"**
```drools
rule "Electronics Cashback"
when
    $input : RuleInput()
    eval($input.getMerchantCategory().equals("electronics"))  // "electronics" == "electronics" ✅
then
    $input.addOffer(new Offer(
        UUID.randomUUID().toString(),
        "CASHBACK",
        "10% cashback on electronics"
    ));
end
```
**Result**: ✅ Offer added

**Rule 3: "High Reward Points"**
```drools
rule "High Reward Points"
when
    $input : RuleInput()
    eval($input.getRewardPoints() > 1000)  // 1200 > 1000 ✅
then
    $input.addOffer(new Offer(
        UUID.randomUUID().toString(),
        "REWARD",
        "Redeem reward points"
    ));
end
```
**Result**: ✅ Offer added

**Rule 4: "High Amount EMI"**
```drools
rule "High Amount EMI"
when
    $input : RuleInput()
    eval($input.getAmount() > 100000)  // 75000 > 100000 ❌
then
    $input.addOffer(new Offer(
        UUID.randomUUID().toString(),
        "EMI",
        "EMI available for high-value transaction"
    ));
end
```
**Result**: ❌ Rule not fired (condition not met)

**Generated Offers**:
```java
[
  Offer(offerId="a1b2c3d4", type="EMI", description="No-cost EMI (high credit score)"),
  Offer(offerId="e5f6g7h8", type="CASHBACK", description="10% cashback on electronics"),
  Offer(offerId="i9j0k1l2", type="REWARD", description="Redeem reward points")
]
```

---

### Step 8: MongoDB Update with Offers

**Location**: `TransactionProcessingService.process()` (continued)

**MongoDB Update**:
```java
txn.setOffers(offers);
txn.setStatus("PROCESSING");
pendingRepo.save(txn);
```

**Updated MongoDB Document**:
```json
{
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "user123",
  "amount": 75000.0,
  "status": "PROCESSING",
  "createdAt": ISODate("2026-05-17T04:00:00.000Z"),
  "offers": [
    {
      "offerId": "a1b2c3d4",
      "type": "EMI",
      "description": "No-cost EMI (high credit score)"
    },
    {
      "offerId": "e5f6g7h8",
      "type": "CASHBACK",
      "description": "10% cashback on electronics"
    },
    {
      "offerId": "i9j0k1l2",
      "type": "REWARD",
      "description": "Redeem reward points"
    }
  ],
  "_class": "com.pos.entity.PendingTransaction"
}
```

**System State**:
- ✅ MongoDB: Transaction updated with offers
- ✅ Status changed: PENDING → PROCESSING
- ✅ Kafka: Message successfully processed (offset committed)
- ⏳ Waiting for user to select offer

---

### Step 9: User Retrieves Offers

**HTTP Request**:
```http
GET /api/v1/transactions/550e8400-e29b-41d4-a716-446655440000/offers HTTP/1.1
Host: localhost:8080
```

**Entry Point**: `TransactionController.getOffers()`
```java
@GetMapping("/{transactionId}/offers")
public ResponseEntity<?> getOffers(@PathVariable String transactionId) {
    return ResponseEntity.ok(transactionService.getOffers(transactionId));
}
```

**Service Logic**: `TransactionService.getOffers()`
```java
public Object getOffers(String transactionId) {
    PendingTransaction txn = pendingRepo.findById(transactionId)
        .orElseThrow(() -> new RuntimeException("Transaction not found"));
    
    return txn.getOffers();
}
```

**HTTP Response**:
```http
HTTP/1.1 200 OK
Content-Type: application/json

[
  {
    "offerId": "a1b2c3d4",
    "type": "EMI",
    "description": "No-cost EMI (high credit score)"
  },
  {
    "offerId": "e5f6g7h8",
    "type": "CASHBACK",
    "description": "10% cashback on electronics"
  },
  {
    "offerId": "i9j0k1l2",
    "type": "REWARD",
    "description": "Redeem reward points"
  }
]
```

---

### Step 10: User Selects Offer

**HTTP Request**:
```http
POST /api/v1/transactions/550e8400-e29b-41d4-a716-446655440000/select-offer HTTP/1.1
Host: localhost:8080
Content-Type: application/json

{
  "offerId": "e5f6g7h8"
}
```

**Entry Point**: `TransactionController.selectOffer()`
```java
@PostMapping("/{transactionId}/select-offer")
public ResponseEntity<?> selectOffer(
        @PathVariable String transactionId,
        @RequestBody SelectOfferRequest request) {
    
    transactionService.selectOffer(transactionId, request.getOfferId());
    return ResponseEntity.ok("Offer selected");
}
```

---

### Step 11: Offer Validation & PostgreSQL Persistence

**Location**: `TransactionService.selectOffer()`

**Code Flow**:
```java
public void selectOffer(String transactionId, String offerId) {
    // 1. Fetch from MongoDB
    PendingTransaction txn = pendingRepo.findById(transactionId)
        .orElseThrow(() -> new RuntimeException("Transaction not found"));
    
    // 2. Validate offer exists
    boolean valid = txn.getOffers().stream()
        .anyMatch(o -> o.getOfferId().equals(offerId));
    
    if (!valid) {
        throw new RuntimeException("Invalid offer");
    }
    // Validation: "e5f6g7h8" exists in offers ✅
    
    // 3. Update status in MongoDB (temporary)
    txn.setStatus("PROCESSING");
    pendingRepo.save(txn);
    
    // 4. Create final transaction entity for PostgreSQL
    TransactionEntity entity = TransactionEntity.builder()
        .transactionId(transactionId)
        .userId(txn.getUserId())
        .amount(txn.getAmount())
        .status("COMPLETED")
        .selectedOffer(offerId)
        .createdAt(txn.getCreatedAt())
        .build();
    
    // 5. Save to PostgreSQL (permanent record)
    transactionRepository.save(entity);
    
    // 6. Remove from MongoDB (cleanup)
    pendingRepo.deleteById(transactionId);
}
```

**PostgreSQL Record Created**:
```sql
INSERT INTO transactions (
    transaction_id, user_id, amount, status, selected_offer, created_at
) VALUES (
    '550e8400-e29b-41d4-a716-446655440000',
    'user123',
    75000.0,
    'COMPLETED',
    'e5f6g7h8',
    '2026-05-17 04:00:00.000'
);
```

**MongoDB Cleanup**:
```javascript
db.pending_transactions.deleteOne({
  _id: "550e8400-e29b-41d4-a716-446655440000"
});
```

**HTTP Response**:
```http
HTTP/1.1 200 OK
Content-Type: text/plain

Offer selected
```

---

### Step 12: Final State

**Database State**:
- ❌ MongoDB: Document deleted (no longer needed)
- ✅ PostgreSQL: Permanent record with status "COMPLETED"

**PostgreSQL Query**:
```sql
SELECT * FROM transactions WHERE transaction_id = '550e8400-e29b-41d4-a716-446655440000';
```

**Result**:
```
transaction_id                        | user_id | amount  | status    | selected_offer | created_at
--------------------------------------|---------|---------|-----------|----------------|-------------------------
550e8400-e29b-41d4-a716-446655440000 | user123 | 75000.0 | COMPLETED | e5f6g7h8       | 2026-05-17 04:00:00.000
```

**Transaction Complete** ✅

---

## Failure Scenario 1: Kafka Publishing Failure

### When It Happens
- Kafka broker is down
- Network connectivity issues
- Topic doesn't exist

### Trace

**Steps 1-3**: Same as happy path (MongoDB write succeeds)

**Step 4: Kafka Publish Fails**
```java
if ("PENDING".equals(status)) {
    TransactionEvent event = TransactionEvent.builder()...build();
    
    try {
        producer.sendTransactionEvent(event);
    } catch (KafkaException ex) {
        // Kafka is down!
        log.error("Failed to publish to Kafka", ex);
        // Exception: org.apache.kafka.common.errors.TimeoutException
    }
}
```

**Impact**:
- ✅ MongoDB: Has PENDING transaction
- ❌ Kafka: No message published
- ❌ Consumer: Never triggered
- ❌ Offers: Never generated

**Result**: Transaction stuck in PENDING state

**Recovery**:
1. **Timeout Service** will eventually cancel it (after 60 seconds)
2. **Manual Intervention**: Admin can retry or cancel

**Prevention**:
- Implement retry logic in producer
- Add circuit breaker pattern
- Monitor Kafka health

---

## Failure Scenario 2: Consumer Processing Failure

### When It Happens
- MongoDB connection lost during processing
- Unexpected runtime exception

### Trace

**Steps 1-5**: Same as happy path (message reaches consumer)

**Step 6: Processing Fails**
```java
public void process(TransactionEvent event) {
    PendingTransaction txn = pendingRepo.findById(event.getTransactionId())
        .orElse(null);
    
    if (txn == null) return;
    if (!"PENDING".equals(txn.getStatus())) return;
    
    try {
        List<Offer> offers = offerService.generateOffers(...);
        
        // MongoDB connection lost!
        txn.setOffers(offers);
        txn.setStatus("PROCESSING");
        pendingRepo.save(txn);  // Throws MongoTimeoutException
        
    } catch (Exception e) {
        log.error("Error processing txn {}", txn.getTransactionId(), e);
        
        // Cleanup: Delete from MongoDB
        pendingRepo.deleteById(txn.getTransactionId());
        
        throw e; // Propagate to consumer
    }
}
```

**Consumer Catches Exception**:
```java
catch (Exception ex) {
    event.setStatus("FAILED");
    log.error("Processing failed for txn {}", event.getTransactionId(), ex);
    throw ex; // Triggers retry mechanism
}
```

**Kafka Error Handler**:
```java
// Configured in KafkaConfig
DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3))

// Retry 1: After 2 seconds
// Retry 2: After 2 seconds
// Retry 3: After 2 seconds
// All failed → Send to DLQ
```

**DLQ Message**:
```json
{
  "topic": "transactions-dlq",
  "key": "550e8400-e29b-41d4-a716-446655440000",
  "value": {
    "transactionId": "550e8400-e29b-41d4-a716-446655440000",
    "userId": "user123",
    "amount": 75000.0,
    "status": "FAILED"
  },
  "headers": {
    "kafka_dlt-original-topic": "transactions",
    "kafka_dlt-exception-message": "MongoTimeoutException: Timed out after 30000 ms"
  }
}
```

**Final State**:
- ❌ MongoDB: Document deleted (cleanup)
- ❌ PostgreSQL: No record
- ✅ Kafka DLQ: Message for manual review

**Recovery**:
- Fix MongoDB connection
- Manually retry from DLQ
- User can create new transaction

---

## Failure Scenario 3: Drools Rules Engine Failure

### When It Happens
- `kmodule.xml` misconfigured
- Rules file syntax error
- Missing dependencies

### Trace

**Steps 1-6**: Same as happy path

**Step 7: Rule Engine Fails**
```java
public List<Offer> generateOffers(String userId, Double amount) {
    Map<String, String> data = datasetService.getUserData(userId);
    
    int creditScore = Integer.parseInt(data.getOrDefault("creditScore", "650"));
    String category = data.getOrDefault("merchantCategory", "general").toLowerCase();
    int rewardPoints = Integer.parseInt(data.getOrDefault("rewardPoints", "0"));
    
    RuleInput input = new RuleInput();
    input.setAmount(amount);
    input.setCreditScore(creditScore);
    input.setMerchantCategory(category);
    input.setRewardPoints(rewardPoints);
    
    KieSession kieSession = null;
    
    try {
        kieSession = kieContainer.newKieSession("rulesSession");
        // Exception: RuntimeException("Failed to create KieSession. Check kmodule.xml")
    } catch (Exception e) {
        throw new RuntimeException("Failed to create KieSession. Check kmodule.xml", e);
    }
    
    // Never reached...
}
```

**Exception Propagates**:
```
OfferService.generateOffers()
  ↓ throws RuntimeException
TransactionProcessingService.process()
  ↓ catches, deletes from MongoDB, re-throws
TransactionConsumer.consume()
  ↓ catches, logs, re-throws
Kafka Error Handler
  ↓ retries 3 times
DLQ
```

**Logs**:
```
ERROR OfferService - Failed to create KieSession. Check kmodule.xml
ERROR TransactionProcessingService - Error processing txn 550e8400-e29b-41d4-a716-446655440000
ERROR TransactionConsumer - Processing failed for txn 550e8400-e29b-41d4-a716-446655440000
```

**Final State**:
- ❌ MongoDB: Document deleted
- ❌ PostgreSQL: No record
- ✅ Kafka DLQ: Message with exception details

**Recovery**:
- Fix `kmodule.xml` configuration
- Restart application
- Manually retry from DLQ

---

## Failure Scenario 4: Invalid Offer Selection

### When It Happens
- User provides non-existent offer ID
- Offer ID from different transaction
- Malicious request

### Trace

**Steps 1-9**: Same as happy path (offers generated and retrieved)

**Step 10: User Selects Invalid Offer**
```http
POST /api/v1/transactions/550e8400-e29b-41d4-a716-446655440000/select-offer
Content-Type: application/json

{
  "offerId": "invalid-offer-id"
}
```

**Step 11: Validation Fails**
```java
public void selectOffer(String transactionId, String offerId) {
    PendingTransaction txn = pendingRepo.findById(transactionId)
        .orElseThrow(() -> new RuntimeException("Transaction not found"));
    
    // Validate offer
    boolean valid = txn.getOffers().stream()
        .anyMatch(o -> o.getOfferId().equals(offerId));
    
    if (!valid) {
        throw new RuntimeException("Invalid offer");
        // Exception thrown here!
    }
    
    // Never reached...
}
```

**HTTP Response**:
```http
HTTP/1.1 500 Internal Server Error
Content-Type: application/json

{
  "timestamp": "2026-05-17T04:05:00.000Z",
  "status": 500,
  "error": "Internal Server Error",
  "message": "Invalid offer",
  "path": "/api/v1/transactions/550e8400-e29b-41d4-a716-446655440000/select-offer"
}
```

**Final State**:
- ✅ MongoDB: Transaction still in PROCESSING state
- ❌ PostgreSQL: No record
- ⏳ User can retry with valid offer ID
- ⏳ Or timeout service will cancel after 60 seconds

**Recovery**:
- User retries with correct offer ID
- Or waits for timeout cancellation

---

## Failure Scenario 5: Timeout Cancellation

### When It Happens
- User doesn't select offer within 60 seconds
- User abandons transaction
- Network issues prevent selection

### Trace

**Steps 1-9**: Same as happy path (offers generated and retrieved)

**Step 10: User Does Nothing**
- Time passes...
- 10 seconds... 20 seconds... 60 seconds...

**Step 11: Timeout Service Triggers**

**Scheduler**: `TransactionTimeoutService` (runs every 10 seconds)
```java
@Scheduled(fixedRate = 10000)
public void cancelTimedOutTransactions() {
    // Calculate cutoff time (60 seconds ago)
    Instant cutoff = Instant.now().minusSeconds(60);
    // cutoff = 2026-05-17T04:01:00.000Z
    
    // Find stale transactions
    List<PendingTransaction> transactions =
        pendingRepo.findByStatusAndCreatedAtBefore("PROCESSING", cutoff);
    
    // Our transaction created at 04:00:00, now it's 04:01:10
    // 04:00:00 < 04:01:00 → Transaction found!
    
    for (PendingTransaction txn : transactions) {
        // Create final transaction with CANCELLED status
        TransactionEntity finalTxn = TransactionEntity.builder()
            .transactionId(txn.getTransactionId())
            .userId(txn.getUserId())
            .amount(txn.getAmount())
            .status("CANCELLED")
            .createdAt(txn.getCreatedAt())
            .build();
        
        // Save to PostgreSQL
        transactionRepo.save(finalTxn);
        
        // Delete from MongoDB
        pendingRepo.deleteById(txn.getTransactionId());
        
        log.warn("Txn {} moved to CANCELLED (Postgres)", txn.getTransactionId());
    }
}
```

**PostgreSQL Record Created**:
```sql
INSERT INTO transactions (
    transaction_id, user_id, amount, status, selected_offer, created_at
) VALUES (
    '550e8400-e29b-41d4-a716-446655440000',
    'user123',
    75000.0,
    'CANCELLED',
    NULL,
    '2026-05-17 04:00:00.000'
);
```

**Final State**:
- ❌ MongoDB: Document deleted
- ✅ PostgreSQL: Record with status "CANCELLED"
- ⏹️ Transaction lifecycle complete (cancelled)

**User Attempts to Select Offer After Timeout**:
```http
POST /api/v1/transactions/550e8400-e29b-41d4-a716-446655440000/select-offer
```

**Response**:
```http
HTTP/1.1 500 Internal Server Error

{
  "message": "Transaction not found"
}
```

**Recovery**:
- User must create new transaction
- No recovery possible (transaction cancelled)

---

## Data Flow Summary

### Success Path Timeline

```
T+0ms:    Client sends POST request
T+10ms:   MongoDB write (PENDING)
T+15ms:   Kafka publish
T+20ms:   HTTP response to client
T+50ms:   Consumer receives message
T+100ms:  Load user data from CSV
T+150ms:  Execute Drools rules (3 rules fired)
T+200ms:  MongoDB update (PROCESSING + offers)
T+250ms:  Consumer commits offset
---
[User reviews offers for 30 seconds]
---
T+30s:    User sends GET /offers request
T+30.01s: MongoDB query returns offers
T+30.02s: HTTP response with offers
---
[User selects offer]
---
T+35s:    User sends POST /select-offer
T+35.01s: Validate offer exists
T+35.02s: PostgreSQL write (COMPLETED)
T+35.03s: MongoDB delete
T+35.04s: HTTP response "Offer selected"
```

### Database State Evolution

```
Time    | MongoDB Status  | PostgreSQL Status | Kafka Status
--------|-----------------|-------------------|-------------
T+0     | -               | -                 | -
T+10ms  | PENDING         | -                 | Message queued
T+200ms | PROCESSING      | -                 | Message processed
T+35s   | (deleted)       | COMPLETED         | Offset committed
```

### Failure Path Timeline

```
T+0ms:    Client sends POST request
T+10ms:   MongoDB write (PENDING)
T+15ms:   Kafka publish
T+20ms:   HTTP response to client
T+50ms:   Consumer receives message
T+100ms:  Load user data from CSV
T+150ms:  Drools fails (RuntimeException)
T+151ms:  MongoDB delete (cleanup)
T+152ms:  Exception thrown to consumer
T+2s:     Retry 1 (fails again)
T+4s:     Retry 2 (fails again)
T+6s:     Retry 3 (fails again)
T+6.1s:   Message sent to DLQ
```

### System Components Interaction

```
┌─────────┐
│ Client  │
└────┬────┘
     │ HTTP
     ▼
┌─────────────────┐
│   Controller    │
└────┬────────────┘
     │
     ▼
┌─────────────────┐     ┌──────────┐
│    Service      │────▶│ MongoDB  │
└────┬────────────┘     └──────────┘
     │
     ▼
┌─────────────────┐
│  Kafka Producer │
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│  Kafka Broker   │
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Kafka Consumer  │
└────┬────────────┘
     │
     ▼
┌─────────────────┐     ┌──────────┐
│  Processing Svc │────▶│ MongoDB  │
└────┬────────────┘     └──────────┘
     │
     ▼
┌─────────────────┐     ┌──────────┐
│  Offer Service  │────▶│  Drools  │
└─────────────────┘     └──────────┘
     │
     ▼
┌─────────────────┐     ┌──────────┐
│    Service      │────▶│PostgreSQL│
└─────────────────┘     └──────────┘
```

---

## Key Takeaways

1. **Asynchronous Processing**: Transaction creation returns immediately; offer generation happens in background
2. **CQRS Pattern**: MongoDB for writes (temporary), PostgreSQL for reads (permanent)
3. **Failure Resilience**: Multiple retry attempts before DLQ
4. **Cleanup Strategy**: Failed transactions deleted from MongoDB to prevent orphans
5. **Timeout Handling**: Automated cancellation after 60 seconds
6. **Idempotency**: Status checks prevent duplicate processing
7. **Audit Trail**: PostgreSQL maintains complete history (COMPLETED or CANCELLED)

---

**End of Transaction Trace**