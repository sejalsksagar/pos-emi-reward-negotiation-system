# Backend Testing Guide - POS EMI & Reward Negotiation System

## Table of Contents
1. [Environment Setup](#1-environment-setup)
2. [End-to-End Testing Flow](#2-end-to-end-testing-flow)
3. [Test Scenarios](#3-test-scenarios)
4. [Verification Checklist](#4-verification-checklist)

---

## 1. Environment Setup

### 1.1 Prerequisites
- Java 17+
- Maven 3.6+
- Docker & Docker Compose
- PostgreSQL 14+
- MongoDB 6+
- Apache Kafka 3.x

### 1.2 Required Services

#### Start All Services with Docker Compose
```bash
docker-compose start
```

#### Start MongoDB (if not using Docker Compose)
```bash
mongod
```

**Verify Services:**
```bash
# Check all containers
docker-compose ps

# Check MongoDB
mongosh mongodb://localhost:27017/posdb
```

### 1.3 Environment Variables

Create `.env` file:
```properties
DB_PASSWORD=your_postgres_password
```

### 1.4 Kafka Topics Setup

```bash
# Create main topic
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic transactions --partitions 3 --replication-factor 1

# Create DLQ topic
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic transactions-dlq --partitions 3 --replication-factor 1

# Verify
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 1.5 Start Application

```bash
# Build
mvn clean install -DskipTests

# Run
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

**Access Points:**
- API: http://localhost:8080
- Swagger UI: http://localhost:8080/swagger-ui.html

---

## 2. End-to-End Testing Flow

### 2.1 Complete Transaction Flow

```
1. POST /api/v1/transactions → Create Transaction
   ↓
2. MongoDB: Save PendingTransaction (status: PENDING)
   ↓
3. Kafka: Publish TransactionEvent to 'transactions' topic
   ↓
4. Consumer: Process event → Generate offers via Drools
   ↓
5. MongoDB: Update PendingTransaction with offers (status: PROCESSING)
   ↓
6. GET /api/v1/transactions/{id}/offers → Retrieve offers
   ↓
7. POST /api/v1/transactions/{id}/select-offer → Select offer
   ↓
8. PostgreSQL: Save final TransactionEntity (status: COMPLETED)
   ↓
9. MongoDB: Delete PendingTransaction (cleanup)
```

### 2.2 State Transitions

| State | Storage | Description |
|-------|---------|-------------|
| PENDING | MongoDB | Initial state, awaiting offer generation |
| PROCESSING | MongoDB | Offers generated, awaiting user selection |
| COMPLETED | PostgreSQL | Offer selected, transaction finalized |
| CANCELLED | PostgreSQL | Timeout expired (60s), no offer selected |
| FAILED | DLQ | Processing error, sent to Dead Letter Queue |

---

## 3. Test Scenarios

### 3.1 Happy Path - High Amount Transaction

**Scenario:** User initiates transaction above threshold (>50,000)

**Test Case 1: EMI Offer Generation**

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U004505", "amount": 250000}'
```

**Request (Swagger UI):**
1. Open http://localhost:8080/swagger-ui.html
2. Expand `POST /api/v1/transactions`
3. Click "Try it out"
4. Enter request body:
```json
{
  "userId": "U004505",
  "amount": 250000
}
```
5. Click "Execute"
6. Copy the transaction ID from the response

**Expected Response:**
```json
"550e8400-e29b-41d4-a716-446655440000"
```

**Kafka Event (Monitor):**
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning
```
Expected output:
```json
{
  "transactionId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "U004505",
  "amount": 250000,
  "status": "PENDING"
}
```

**MongoDB State Check:**
```javascript
db.pending_transactions.findOne({"transactionId": "550e8400-e29b-41d4-a716-446655440000"})
```
Expected:
```json
{
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "transactionId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "U004505",
  "amount": 250000,
  "status": "PENDING",
  "createdAt": "2026-05-17T09:00:00.000Z",
  "offers": null
}
```

**Wait 2-3 seconds for consumer processing**

**Check Offers (cURL):**
```bash
curl http://localhost:8080/api/v1/transactions/550e8400-e29b-41d4-a716-446655440000/offers
```

**Check Offers (Swagger UI):**
1. Expand `GET /api/v1/transactions/{transactionId}/offers`
2. Click "Try it out"
3. Enter the transaction ID: `550e8400-e29b-41d4-a716-446655440000`
4. Click "Execute"
5. View the offers in the response

**Expected Response:**
```json
[
  {
    "offerId": "offer-uuid-1",
    "type": "EMI",
    "description": "EMI available for high-value transaction"
  },
  {
    "offerId": "offer-uuid-2",
    "type": "EMI",
    "description": "No-cost EMI (high credit score)"
  }
]
```

**MongoDB State After Processing:**
```json
{
  "transactionId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "PROCESSING",
  "offers": [
    {"offerId": "offer-uuid-1", "type": "EMI", "description": "..."},
    {"offerId": "offer-uuid-2", "type": "EMI", "description": "..."}
  ]
}
```

**Select Offer (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions/550e8400-e29b-41d4-a716-446655440000/select-offer -H "Content-Type: application/json" -d '{"offerId": "offer-uuid-1"}'
```

**Select Offer (Swagger UI):**
1. Expand `POST /api/v1/transactions/{transactionId}/select-offer`
2. Click "Try it out"
3. Enter the transaction ID: `550e8400-e29b-41d4-a716-446655440000`
4. Enter request body:
```json
{
  "offerId": "offer-uuid-1"
}
```
5. Click "Execute"
6. Verify "Offer selected" response

**Expected Response:**
```json
"Offer selected"
```

**PostgreSQL State Check:**
```sql
SELECT * FROM transactions WHERE transaction_id = '550e8400-e29b-41d4-a716-446655440000';
```
Expected:
```
transaction_id | user_id  | amount  | status    | selected_offer | created_at
---------------|----------|---------|-----------|----------------|------------
550e8400...    | U004505  | 250000  | COMPLETED | offer-uuid-1   | 2026-05-17...
```

**MongoDB Cleanup Verification:**
```javascript
db.pending_transactions.findOne({"transactionId": "550e8400-e29b-41d4-a716-446655440000"})
// Should return null (deleted)
```

**Logs to Verify:**
```
INFO  - Received transaction event: TransactionEvent(transactionId=550e8400...)
INFO  - Rules fired: 2
INFO  - Transaction 550e8400... processed successfully
```

**Success Criteria:**
- ✅ Transaction ID returned
- ✅ Kafka event published
- ✅ MongoDB record created with PENDING status
- ✅ Consumer processed event
- ✅ Offers generated (2 EMI offers)
- ✅ MongoDB updated to PROCESSING with offers
- ✅ Offer selection successful
- ✅ PostgreSQL record created with COMPLETED status
- ✅ MongoDB record deleted

---

### 3.2 Happy Path - Reward Offer Generation

**Test Case 2: High Reward Points User**

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U001234", "amount": 150000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "U001234", "amount": 150000}`
3. Execute and copy transaction ID

**Expected Offers (if user has >1000 reward points):**
```json
[
  {
    "offerId": "offer-uuid-3",
    "type": "EMI",
    "description": "EMI available for high-value transaction"
  },
  {
    "offerId": "offer-uuid-4",
    "type": "REWARD",
    "description": "Redeem reward points"
  }
]
```

**Success Criteria:**
- ✅ REWARD offer included in response
- ✅ Drools rule "High Reward Points" fired

---

### 3.3 Edge Case - Amount Below Threshold

**Test Case 3: Low Amount Transaction**

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U004505", "amount": 25000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "U004505", "amount": 25000}`
3. Execute and note immediate COMPLETED status

**Expected Response:**
```json
"transaction-id-xyz"
```

**MongoDB State:**
```json
{
  "transactionId": "transaction-id-xyz",
  "userId": "U004505",
  "amount": 25000,
  "status": "COMPLETED",
  "offers": null
}
```

**Kafka Event:**
- ❌ No event published (amount < 50000)

**PostgreSQL State:**
- ❌ No record created (transaction completed immediately)

**Success Criteria:**
- ✅ Transaction created with COMPLETED status
- ✅ No Kafka event published
- ✅ No offer generation triggered
- ✅ Direct completion without processing

---

### 3.4 Edge Case - Invalid User ID

**Test Case 4: Non-existent User**

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "INVALID_USER", "amount": 100000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "INVALID_USER", "amount": 100000}`
3. Execute and verify default offers generated

**Expected Behavior:**
- Transaction created in MongoDB
- Kafka event published
- Consumer processes with default values (creditScore: 650, category: general, rewardPoints: 0)

**Expected Offers:**
```json
[
  {
    "offerId": "offer-uuid-5",
    "type": "EMI",
    "description": "EMI available for high-value transaction"
  }
]
```

**Success Criteria:**
- ✅ System handles gracefully with defaults
- ✅ At least one offer generated

---

### 3.5 Edge Case - Missing Payload Fields

**Test Case 5: Missing Amount**

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U004505"}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "U004505"}` (missing amount)
3. Execute and verify validation error

**Expected Response:**
```json
{
  "error": "Bad Request",
  "message": "amount is required"
}
```

**Test Case 6: Missing User ID**

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"amount": 100000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"amount": 100000}` (missing userId)
3. Execute and verify validation error

**Expected Response:**
```json
{
  "error": "Bad Request",
  "message": "userId is required"
}
```

**Success Criteria:**
- ✅ Validation error returned
- ✅ No database records created
- ✅ No Kafka events published

---

### 3.6 Edge Case - Duplicate Requests

**Test Case 7: Idempotency Check**

**Request 1 (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U004505", "amount": 100000}'
```

**Request 2 (cURL - Immediate):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U004505", "amount": 100000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Execute same request twice with: `{"userId": "U004505", "amount": 100000}`
3. Verify two different transaction IDs returned

**Expected Behavior:**
- Two separate transactions created (different IDs)
- System does NOT prevent duplicates (by design)

**Success Criteria:**
- ✅ Both transactions processed independently
- ✅ Different transaction IDs generated

---

### 3.7 Edge Case - Unsupported Merchant Category

**Test Case 8: Unknown Category**

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U999999", "amount": 100000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "U999999", "amount": 100000}`
3. Execute and verify only amount-based offers

**Expected Behavior:**
- Default category "general" used
- Only amount-based rules fire

**Expected Offers:**
```json
[
  {
    "offerId": "offer-uuid-6",
    "type": "EMI",
    "description": "EMI available for high-value transaction"
  }
]
```

**Success Criteria:**
- ✅ Transaction processed with defaults
- ✅ No category-specific offers

---

### 3.8 Edge Case - No Eligible Offers

**Test Case 9: Low Amount, Low Credit Score**

**Setup:** User with creditScore < 700, amount < 100000, no reward points

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U_LOW_CREDIT", "amount": 75000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "U_LOW_CREDIT", "amount": 75000}`
3. Execute and verify empty offers array

**Expected Offers:**
```json
[]
```

**MongoDB State:**
```json
{
  "status": "PROCESSING",
  "offers": []
}
```

**Success Criteria:**
- ✅ Empty offers array returned
- ✅ No errors thrown
- ✅ Transaction remains in PROCESSING state

---

### 3.9 Failure Scenario - Kafka Unavailable

**Test Case 10: Kafka Down**

**Setup:**
```bash
docker stop kafka
```

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U004505", "amount": 100000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "U004505", "amount": 100000}`
3. Execute and verify error in logs (Kafka unavailable)

**Expected Behavior:**
- MongoDB record created
- Kafka send fails
- Error logged

**Logs:**
```
ERROR - Failed to send transaction event: Connection refused
```

**MongoDB State:**
```json
{
  "status": "PENDING",
  "offers": null
}
```

**Success Criteria:**
- ✅ Transaction created in MongoDB
- ✅ Error logged
- ✅ Transaction stuck in PENDING (no processing)

**Recovery:**
```bash
docker start kafka
# Wait for Kafka to be ready
# Manually republish or implement retry mechanism
```

---

### 3.10 Failure Scenario - Consumer Failure

**Test Case 11: Processing Exception**

**Setup:** Uncomment line 24 in TransactionConsumer.java:
```java
throw new RuntimeException("Test failure");
```

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U004505", "amount": 100000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "U004505", "amount": 100000}`
3. Execute and monitor logs for retry attempts and DLQ

**Expected Behavior:**
1. Consumer receives event
2. Exception thrown
3. Retry 3 times (2-second intervals)
4. After 3 failures → sent to DLQ

**Logs:**
```
ERROR - Processing failed for txn 550e8400...
ERROR - Retry attempt 1/3
ERROR - Retry attempt 2/3
ERROR - Retry attempt 3/3
INFO  - Message sent to DLQ: transactions-dlq
```

**DLQ Verification:**
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions-dlq --from-beginning
```

**MongoDB State:**
```json
{
  "status": "PENDING",
  "offers": null
}
```

**Success Criteria:**
- ✅ 3 retry attempts made
- ✅ Message sent to DLQ after failures
- ✅ Original message not reprocessed
- ✅ MongoDB record remains in PENDING

---

### 3.11 Failure Scenario - Drools Rule Failure

**Test Case 12: Invalid Rule Configuration**

**Setup:** Corrupt offer-rules.drl temporarily

**Expected Behavior:**
- KieSession creation fails
- RuntimeException thrown
- Sent to DLQ after retries

**Logs:**
```
ERROR - Failed to create KieSession. Check kmodule.xml
ERROR - Processing failed for txn...
```

**Success Criteria:**
- ✅ Error caught and logged
- ✅ Transaction sent to DLQ
- ✅ MongoDB record deleted (cleanup)

---

### 3.12 Failure Scenario - Database Failure

**Test Case 13: MongoDB Down**

**Setup:**
```bash
docker stop mongodb-pos
```

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U004505", "amount": 100000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "U004505", "amount": 100000}`
3. Execute and verify database error response

**Expected Response:**
```json
{
  "error": "Internal Server Error",
  "message": "Database connection failed"
}
```

**Success Criteria:**
- ✅ Error returned to client
- ✅ No partial data created
- ✅ No Kafka event published

**Test Case 14: PostgreSQL Down**

**Setup:**
```bash
docker stop postgres-pos
```

**Scenario:** Try to select offer

**Expected Behavior:**
- Offer selection fails
- Error logged
- MongoDB record remains

**Success Criteria:**
- ✅ Error returned to client
- ✅ Transaction remains in PROCESSING state

---

### 3.13 Failure Scenario - Timeout Expiry

**Test Case 15: No Offer Selection Within 60 Seconds**

**Request (cURL):**
```bash
curl -X POST http://localhost:8080/api/v1/transactions -H "Content-Type: application/json" -d '{"userId": "U004505", "amount": 100000}'
```

**Request (Swagger UI):**
1. Use `POST /api/v1/transactions`
2. Enter: `{"userId": "U004505", "amount": 100000}`
3. Execute and copy transaction ID
4. **Wait 60+ seconds without selecting offer**
5. Check PostgreSQL for CANCELLED status

**Expected Behavior:**
- Timeout service runs every 10 seconds
- Detects transaction in PROCESSING state older than 60s
- Moves to PostgreSQL with CANCELLED status
- Deletes from MongoDB

**Logs:**
```
WARN - Txn 550e8400... moved to CANCELLED (Postgres)
```

**PostgreSQL State:**
```sql
SELECT * FROM transactions WHERE transaction_id = '550e8400...';
```
Expected:
```
status: CANCELLED
selected_offer: NULL
```

**MongoDB State:**
```javascript
db.pending_transactions.findOne({"transactionId": "550e8400..."})
// Returns null
```

**Success Criteria:**
- ✅ Transaction auto-cancelled after 60s
- ✅ Moved to PostgreSQL with CANCELLED status
- ✅ Removed from MongoDB
- ✅ Warning logged

---

### 3.14 Failure Scenario - DLQ Handling

**Test Case 16: Monitor DLQ**

**Monitor DLQ Topic:**
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions-dlq --from-beginning
```

**Expected Messages:**
- Failed transactions after 3 retries
- Original event payload preserved

**Manual Recovery Process:**
1. Identify failed transaction in DLQ
2. Fix root cause (e.g., restore service)
3. Manually reprocess:
```bash
# Get transaction ID from DLQ
# Republish to main topic
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic transactions
# Paste the event JSON
```

**Success Criteria:**
- ✅ DLQ contains failed messages
- ✅ Messages can be manually reprocessed
- ✅ No data loss

---

### 3.15 Persistence Validation

**Test Case 17: MongoDB Intermediate State**

**Query All Pending Transactions:**
```javascript
db.pending_transactions.find().pretty()
```

**Expected Fields:**
- transactionId (String, UUID)
- userId (String)
- amount (Double)
- status (String: PENDING, PROCESSING)
- createdAt (ISODate)
- offers (Array or null)

**Validation Queries:**
```javascript
// Count by status
db.pending_transactions.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } }
])

// Find old pending transactions
db.pending_transactions.find({
  status: "PENDING",
  createdAt: { $lt: new Date(Date.now() - 60000) }
})

// Find transactions with offers
db.pending_transactions.find({ offers: { $ne: null } })
```

**Test Case 18: PostgreSQL Final State**

**Query All Completed Transactions:**
```sql
SELECT * FROM transactions ORDER BY created_at DESC;
```

**Expected Fields:**
- transaction_id (VARCHAR, UUID)
- user_id (VARCHAR)
- amount (DOUBLE PRECISION)
- status (VARCHAR: COMPLETED, CANCELLED)
- selected_offer (VARCHAR, nullable)
- created_at (TIMESTAMP)

**Validation Queries:**
```sql
-- Count by status
SELECT status, COUNT(*) FROM transactions GROUP BY status;

-- Find completed transactions
SELECT * FROM transactions WHERE status = 'COMPLETED';

-- Find cancelled transactions
SELECT * FROM transactions WHERE status = 'CANCELLED';

-- Find transactions with selected offers
SELECT * FROM transactions WHERE selected_offer IS NOT NULL;

-- Check for orphaned records (should not exist in both DBs)
SELECT t.transaction_id 
FROM transactions t
WHERE EXISTS (
  SELECT 1 FROM pending_transactions p 
  WHERE p.transaction_id = t.transaction_id
);
```

---

## 4. Verification Checklist

### 4.1 Pre-Test Checklist

- [ ] PostgreSQL running and accessible
- [ ] MongoDB running and accessible
- [ ] Kafka & Zookeeper running
- [ ] Topics created (transactions, transactions-dlq)
- [ ] Application started successfully
- [ ] Swagger UI accessible
- [ ] Environment variables configured

### 4.2 During Test Checklist

- [ ] Monitor application logs: `tail -f logs/application.log`
- [ ] Monitor Kafka consumer: `kafka-console-consumer.sh`
- [ ] Check MongoDB state after each operation
- [ ] Check PostgreSQL state after completion
- [ ] Verify Kafka events published
- [ ] Check DLQ for failed messages

### 4.3 Post-Test Checklist

- [ ] All happy path scenarios passed
- [ ] All edge cases handled correctly
- [ ] All failure scenarios tested
- [ ] DLQ contains expected failed messages
- [ ] No orphaned records in MongoDB
- [ ] PostgreSQL contains all final states
- [ ] Timeout mechanism working
- [ ] Retry mechanism working (3 attempts)
- [ ] Logs contain expected entries

### 4.4 Performance Checklist

- [ ] Transaction creation < 100ms
- [ ] Offer generation < 2 seconds
- [ ] Offer selection < 100ms
- [ ] Kafka event processing < 1 second
- [ ] Timeout check runs every 10 seconds

### 4.5 Data Integrity Checklist

- [ ] No duplicate transactions in PostgreSQL
- [ ] MongoDB cleaned up after completion
- [ ] All COMPLETED transactions have selected_offer
- [ ] All CANCELLED transactions have no selected_offer
- [ ] Timestamps accurate across systems

---

## 5. Common Issues & Troubleshooting

### Issue 1: Kafka Connection Refused
**Symptom:** `Connection refused: localhost:9092`
**Solution:**
```bash
# Check Kafka status
docker ps | grep kafka
# Restart Kafka
docker restart kafka
# Wait 30 seconds for startup
```

### Issue 2: MongoDB Connection Timeout
**Symptom:** `MongoTimeoutException`
**Solution:**
```bash
# Check MongoDB status
docker ps | grep mongo
# Restart MongoDB
docker restart mongodb-pos
```

### Issue 3: PostgreSQL Authentication Failed
**Symptom:** `PSQLException: password authentication failed`
**Solution:**
- Verify DB_PASSWORD in .env file
- Check application.properties configuration

### Issue 4: No Offers Generated
**Symptom:** Empty offers array
**Solution:**
- Check user data in CSV file
- Verify Drools rules in offer-rules.drl
- Check logs for "Rules fired: 0"

### Issue 5: Consumer Not Processing
**Symptom:** Messages in Kafka but not processed
**Solution:**
```bash
# Check consumer group
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group pos-group --describe
# Check for lag
# Restart application if needed
```

### Issue 6: Timeout Not Working
**Symptom:** Old transactions not cancelled
**Solution:**
- Verify @Scheduled annotation enabled
- Check application logs for timeout service execution
- Verify cutoff time calculation (60 seconds)

---

## 6. Test Data Reference

### Sample User IDs with Different Profiles

| User ID | Credit Score | Merchant Category | Reward Points | Expected Offers |
|---------|--------------|-------------------|---------------|-----------------|
| U004505 | 750 | electronics | 1500 | EMI, CASHBACK, REWARD |
| U001234 | 680 | general | 1200 | EMI, REWARD |
| U_LOW_CREDIT | 650 | general | 0 | EMI (amount-based only) |
| U_ELECTRONICS | 720 | electronics | 500 | EMI, CASHBACK |
| INVALID_USER | 650 (default) | general (default) | 0 (default) | EMI (amount-based only) |

### Amount Thresholds

| Amount | Behavior |
|--------|----------|
| < 50,000 | Direct COMPLETED, no Kafka event |
| 50,000 - 100,000 | PENDING → Kafka → Offers |
| > 100,000 | PENDING → Kafka → Multiple offers (EMI rule fires) |

---

## 7. API Reference

### POST /api/v1/transactions
Create new transaction

**Request Body:**
```json
{
  "userId": "string",
  "amount": number
}
```

**Response:** `string` (transaction ID)

### GET /api/v1/transactions/{transactionId}/offers
Get offers for transaction

**Response:**
```json
[
  {
    "offerId": "string",
    "type": "string",
    "description": "string"
  }
]
```

### POST /api/v1/transactions/{transactionId}/select-offer
Select an offer

**Request Body:**
```json
{
  "offerId": "string"
}
```

**Response:** `"Offer selected"`

### GET /api/v1/transactions/{transactionId}
Get final transaction details

**Response:**
```json
{
  "transactionId": "string",
  "userId": "string",
  "amount": number,
  "status": "string",
  "selectedOffer": "string",
  "createdAt": "timestamp"
}
```

### GET /api/v1/transactions/all
Get all transactions (PostgreSQL)

**Response:** Array of transaction entities

---

## 8. Monitoring Commands

### Real-time Kafka Monitoring
```bash
# Monitor main topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning

# Monitor DLQ
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions-dlq --from-beginning

# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group pos-group --describe
```

### Database Monitoring
```bash
# MongoDB - Watch changes
mongosh mongodb://localhost:27017/posdb --eval "db.pending_transactions.watch()"

# PostgreSQL - Tail logs
docker logs -f postgres-pos

# PostgreSQL - Active connections
psql -h localhost -U postgres -d testdb -c "SELECT * FROM pg_stat_activity;"
```

### Application Monitoring
```bash
# Tail application logs
tail -f logs/application.log

# Filter for errors
tail -f logs/application.log | grep ERROR

# Filter for specific transaction
tail -f logs/application.log | grep "550e8400"
```

---

## Conclusion

This testing guide covers comprehensive scenarios for the POS EMI & Reward Negotiation System. Follow the test cases sequentially to validate all system components including:

- ✅ CQRS pattern (MongoDB write, PostgreSQL read)
- ✅ Kafka event-driven architecture
- ✅ Drools rule engine integration
- ✅ Saga-like timeout handling
- ✅ DLQ mechanism for failure recovery
- ✅ Retry behavior (3 attempts with 2s backoff)

For production deployment, ensure all test scenarios pass and monitoring is in place for Kafka lag, database connections, and DLQ messages.