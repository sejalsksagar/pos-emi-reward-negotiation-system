# Test Execution Commands

## Prerequisites
Ensure you're in the project root directory.

## ⚠️ Important: JaCoCo Plugin Added
The `pom.xml` has been updated with JaCoCo and Surefire plugins for test coverage and reporting.

## Run All Tests

### Using Maven Wrapper (Recommended)
```bash
# Windows (PowerShell/CMD)
.\mvnw.cmd clean test

# Windows (Git Bash)
./mvnw clean test
```

### Using Maven (if installed globally)
```bash
mvn clean test
```

---

## Run Specific Test Classes

### Run TransactionServiceTest
```bash
.\mvnw.cmd test -Dtest=TransactionServiceTest
```

### Run TransactionProcessingServiceTest
```bash
.\mvnw.cmd test -Dtest=TransactionProcessingServiceTest
```

### Run OfferServiceTest
```bash
.\mvnw.cmd test -Dtest=OfferServiceTest
```

### Run TransactionTimeoutServiceTest
```bash
.\mvnw.cmd test -Dtest=TransactionTimeoutServiceTest
```

### Run TransactionProducerTest
```bash
.\mvnw.cmd test -Dtest=TransactionProducerTest
```

---

## Run Multiple Specific Test Classes
```bash
.\mvnw.cmd test -Dtest=TransactionServiceTest,OfferServiceTest
```

---

## Run Specific Test Methods

### Single Test Method
```bash
.\mvnw.cmd test -Dtest=TransactionServiceTest#createTransaction_HighValue_ShouldPublishKafkaEvent
```

### Multiple Test Methods from Same Class
```bash
.\mvnw.cmd test -Dtest=TransactionServiceTest#createTransaction_HighValue_ShouldPublishKafkaEvent+createTransaction_LowValue_ShouldNotPublishKafkaEvent
```

---

## Run Tests with Coverage Report

### Generate JaCoCo Coverage Report (NOW WORKING!)
```bash
.\mvnw.cmd clean test jacoco:report
```

**View Report:**
Open `target/site/jacoco/index.html` in your browser

### Alternative: Coverage Report Generated Automatically
```bash
.\mvnw.cmd clean test
```
The JaCoCo plugin is configured to automatically generate reports during the test phase.
Report location: `target/site/jacoco/index.html`

---

## Run Tests in Parallel (Faster Execution)
```bash
.\mvnw.cmd test -T 4
```
(Uses 4 threads)

---

## Run Tests with Verbose Output
```bash
.\mvnw.cmd test -X
```

---

## Run Tests and Skip Compilation
```bash
.\mvnw.cmd test -DskipTests=false
```

---

## Run Only Service Tests
```bash
.\mvnw.cmd test -Dtest=*ServiceTest
```

---

## Run Only Kafka Tests
```bash
.\mvnw.cmd test -Dtest=*ProducerTest
```

---

## Run Tests with Specific Profile
```bash
.\mvnw.cmd test -Ptest
```

---

## Continuous Test Execution (Watch Mode)
```bash
.\mvnw.cmd test -Dtest=TransactionServiceTest -Dsurefire.rerunFailingTestsCount=2
```

---

## Run Tests and Generate Reports

### Surefire Report
```bash
.\mvnw.cmd clean test surefire-report:report
```
**View Report:** `target/site/surefire-report.html`

### Combined Coverage + Report
```bash
.\mvnw.cmd clean test surefire-report:report
```
Both JaCoCo and Surefire reports will be generated automatically.

---

## Troubleshooting

### If Tests Fail to Run

1. **Clean and Rebuild:**
   ```bash
   .\mvnw.cmd clean install -DskipTests
   .\mvnw.cmd test
   ```

2. **Check Java Version:**
   ```bash
   java -version
   ```
   (Should be Java 17)

3. **Update Dependencies:**
   ```bash
   .\mvnw.cmd dependency:resolve
   ```

4. **Clear Maven Cache:**
   ```bash
   .\mvnw.cmd dependency:purge-local-repository
   ```

### If Specific Test Fails

**Run with Stack Trace:**
```bash
.\mvnw.cmd test -Dtest=TransactionServiceTest -e
```

**Run with Debug Logging:**
```bash
.\mvnw.cmd test -Dtest=TransactionServiceTest -Dorg.slf4j.simpleLogger.defaultLogLevel=debug
```

### If JaCoCo Plugin Error Occurs

The pom.xml has been updated with JaCoCo plugin. If you still get errors:

1. **Verify Plugin Installation:**
   ```bash
   .\mvnw.cmd dependency:tree | findstr jacoco
   ```

2. **Force Update:**
   ```bash
   .\mvnw.cmd clean install -U
   ```

---

## IDE Integration

### IntelliJ IDEA
1. Right-click on test class → "Run 'TestClassName'"
2. Or use keyboard shortcut: `Ctrl+Shift+F10`

### VS Code
1. Install "Java Test Runner" extension
2. Click the play button next to test methods
3. Or use Command Palette: `Java: Run Tests`

### Eclipse
1. Right-click on test class → "Run As" → "JUnit Test"
2. Or use keyboard shortcut: `Alt+Shift+X, T`

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `.\mvnw.cmd test` | Run all tests |
| `.\mvnw.cmd test -Dtest=ClassName` | Run specific test class |
| `.\mvnw.cmd test -Dtest=ClassName#methodName` | Run specific test method |
| `.\mvnw.cmd clean test` | Clean and run all tests |
| `.\mvnw.cmd clean test jacoco:report` | Run tests with coverage (FIXED!) |
| `.\mvnw.cmd test -T 4` | Run tests in parallel (4 threads) |
| `.\mvnw.cmd test -X` | Run with verbose output |

---

## Expected Output

### Successful Test Run
```
[INFO] -------------------------------------------------------
[INFO]  T E S T S
[INFO] -------------------------------------------------------
[INFO] Running com.pos.service.TransactionServiceTest
[INFO] Tests run: 20, Failures: 0, Errors: 0, Skipped: 0
[INFO] Running com.pos.service.TransactionProcessingServiceTest
[INFO] Tests run: 25, Failures: 0, Errors: 0, Skipped: 0
[INFO] Running com.pos.service.OfferServiceTest
[INFO] Tests run: 35, Failures: 0, Errors: 0, Skipped: 0
[INFO] Running com.pos.service.TransactionTimeoutServiceTest
[INFO] Tests run: 30, Failures: 0, Errors: 0, Skipped: 0
[INFO] Running com.pos.kafka.producer.TransactionProducerTest
[INFO] Tests run: 35, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] Results:
[INFO] 
[INFO] Tests run: 145, Failures: 0, Errors: 0, Skipped: 0
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

---

## Performance Benchmarks

Expected execution times (approximate):
- **All tests:** 10-30 seconds
- **Single test class:** 2-5 seconds
- **Single test method:** <1 second

---

## CI/CD Integration

### GitHub Actions
```yaml
- name: Run Tests
  run: ./mvnw test
```

### Jenkins
```groovy
stage('Test') {
    steps {
        sh './mvnw clean test'
    }
}
```

### GitLab CI
```yaml
test:
  script:
    - ./mvnw test
```

---

## Notes

- Tests use **in-memory mocks** - no external dependencies required
- Tests are **isolated** - can run in any order
- Tests are **fast** - no database or Kafka setup needed
- All tests use **JUnit 5** and **Mockito**
- Coverage reports generated in `target/site/jacoco/`
- **JaCoCo plugin is now configured** - coverage reports work!

---

## Next Steps After Running Tests

1. **Review Coverage Report:**
   ```bash
   start target/site/jacoco/index.html
   ```

2. **Check Test Results:**
   ```bash
   start target/surefire-reports/index.html
   ```

3. **Fix Any Failures:**
   - Check stack traces in console output
   - Review test logs in `target/surefire-reports/`
   - Run specific failing test with `-X` flag for details

---

## What Was Fixed

✅ **JaCoCo Plugin Added to pom.xml**
- Version: 0.8.11
- Automatically generates coverage reports during test phase
- Report location: `target/site/jacoco/index.html`

✅ **Maven Surefire Plugin Configured**
- Version: 3.2.5
- Properly configured to run all test files

Now you can run: `.\mvnw.cmd clean test jacoco:report` without errors!

---

## Support

For issues or questions:
1. Check test output for error messages
2. Review `TEST_SUMMARY.md` for test documentation
3. Verify all dependencies are resolved: `.\mvnw.cmd dependency:tree`