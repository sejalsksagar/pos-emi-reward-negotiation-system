package com.pos.service;

import com.pos.entity.PendingTransaction;
import com.pos.entity.TransactionEntity;
import com.pos.repository.PendingTransactionRepository;
import com.pos.repository.TransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("TransactionTimeoutService Tests - Timeout Cancellation")
class TransactionTimeoutServiceTest {

    @Mock
    private PendingTransactionRepository pendingRepo;

    @Mock
    private TransactionRepository transactionRepo;

    @InjectMocks
    private TransactionTimeoutService timeoutService;

    private PendingTransaction timedOutTransaction;
    private Instant cutoffTime;

    @BeforeEach
    void setUp() {
        // Current time minus 60 seconds (cutoff)
        cutoffTime = Instant.now().minusSeconds(60);

        // Transaction created 90 seconds ago (should timeout)
        timedOutTransaction = PendingTransaction.builder()
                .transactionId("txn123")
                .userId("user123")
                .amount(60000.0)
                .status("PROCESSING")
                .createdAt(Instant.now().minusSeconds(90))
                .build();
    }

    // ==================== SUCCESSFUL TIMEOUT CANCELLATION TESTS ====================

    @Test
    @DisplayName("Should cancel single timed-out transaction")
    void cancelTimedOutTransactions_SingleTimeout_ShouldCancel() {
        // Arrange
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        // Verify transaction saved to PostgreSQL with CANCELLED status
        ArgumentCaptor<TransactionEntity> entityCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(transactionRepo).save(entityCaptor.capture());

        TransactionEntity savedEntity = entityCaptor.getValue();
        assertThat(savedEntity.getTransactionId()).isEqualTo("txn123");
        assertThat(savedEntity.getUserId()).isEqualTo("user123");
        assertThat(savedEntity.getAmount()).isEqualTo(60000.0);
        assertThat(savedEntity.getStatus()).isEqualTo("CANCELLED");
        assertThat(savedEntity.getCreatedAt()).isEqualTo(timedOutTransaction.getCreatedAt());

        // Verify transaction deleted from MongoDB
        verify(pendingRepo).deleteById("txn123");
    }

    @Test
    @DisplayName("Should cancel multiple timed-out transactions")
    void cancelTimedOutTransactions_MultipleTimeouts_ShouldCancelAll() {
        // Arrange
        PendingTransaction txn1 = PendingTransaction.builder()
                .transactionId("txn1")
                .userId("user1")
                .amount(50000.0)
                .status("PROCESSING")
                .createdAt(Instant.now().minusSeconds(120))
                .build();

        PendingTransaction txn2 = PendingTransaction.builder()
                .transactionId("txn2")
                .userId("user2")
                .amount(75000.0)
                .status("PROCESSING")
                .createdAt(Instant.now().minusSeconds(90))
                .build();

        PendingTransaction txn3 = PendingTransaction.builder()
                .transactionId("txn3")
                .userId("user3")
                .amount(100000.0)
                .status("PROCESSING")
                .createdAt(Instant.now().minusSeconds(61))
                .build();

        List<PendingTransaction> timedOutTransactions = Arrays.asList(txn1, txn2, txn3);
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(timedOutTransactions);

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        verify(transactionRepo, times(3)).save(any(TransactionEntity.class));
        verify(pendingRepo).deleteById("txn1");
        verify(pendingRepo).deleteById("txn2");
        verify(pendingRepo).deleteById("txn3");
    }

    @Test
    @DisplayName("Should handle empty list of timed-out transactions")
    void cancelTimedOutTransactions_NoTimeouts_ShouldDoNothing() {
        // Arrange
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.emptyList());

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        verify(transactionRepo, never()).save(any());
        verify(pendingRepo, never()).deleteById(anyString());
    }

    @Test
    @DisplayName("Should use correct cutoff time (60 seconds)")
    void cancelTimedOutTransactions_CutoffTime_ShouldUse60Seconds() {
        // Arrange
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.emptyList());

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(pendingRepo).findByStatusAndCreatedAtBefore(eq("PROCESSING"), cutoffCaptor.capture());

        Instant capturedCutoff = cutoffCaptor.getValue();
        Instant expectedCutoff = Instant.now().minusSeconds(60);

        // Allow 1 second tolerance for test execution time
        assertThat(capturedCutoff).isBetween(
                expectedCutoff.minusSeconds(1),
                expectedCutoff.plusSeconds(1)
        );
    }

    @Test
    @DisplayName("Should only query PROCESSING status transactions")
    void cancelTimedOutTransactions_StatusFilter_ShouldOnlyQueryProcessing() {
        // Arrange
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.emptyList());

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        verify(pendingRepo).findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class));
        verify(pendingRepo, never()).findByStatusAndCreatedAtBefore(eq("PENDING"), any(Instant.class));
        verify(pendingRepo, never()).findByStatusAndCreatedAtBefore(eq("COMPLETED"), any(Instant.class));
    }

    // ==================== TRANSACTION METADATA PRESERVATION TESTS ====================

    @Test
    @DisplayName("Should preserve all transaction metadata when cancelling")
    void cancelTimedOutTransactions_PreserveMetadata_ShouldKeepAllFields() {
        // Arrange
        Instant originalCreatedAt = Instant.now().minusSeconds(120);
        timedOutTransaction.setCreatedAt(originalCreatedAt);
        timedOutTransaction.setUserId("special-user-123");
        timedOutTransaction.setAmount(123456.78);

        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        ArgumentCaptor<TransactionEntity> entityCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(transactionRepo).save(entityCaptor.capture());

        TransactionEntity savedEntity = entityCaptor.getValue();
        assertThat(savedEntity.getTransactionId()).isEqualTo("txn123");
        assertThat(savedEntity.getUserId()).isEqualTo("special-user-123");
        assertThat(savedEntity.getAmount()).isEqualTo(123456.78);
        assertThat(savedEntity.getCreatedAt()).isEqualTo(originalCreatedAt);
        assertThat(savedEntity.getStatus()).isEqualTo("CANCELLED");
    }

    @Test
    @DisplayName("Should not set selectedOffer for cancelled transactions")
    void cancelTimedOutTransactions_NoSelectedOffer_ShouldBeNull() {
        // Arrange
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        ArgumentCaptor<TransactionEntity> entityCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(transactionRepo).save(entityCaptor.capture());

        TransactionEntity savedEntity = entityCaptor.getValue();
        assertThat(savedEntity.getSelectedOffer()).isNull();
    }

    // ==================== ERROR HANDLING TESTS ====================

    @Test
    @DisplayName("Should handle PostgreSQL save failure")
    void cancelTimedOutTransactions_PostgresFailure_ShouldThrowException() {
        // Arrange
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));
        when(transactionRepo.save(any())).thenThrow(new RuntimeException("PostgreSQL error"));

        // Act & Assert
        assertThatThrownBy(() -> timeoutService.cancelTimedOutTransactions())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("PostgreSQL error");

        // Verify delete was not called due to save failure
        verify(pendingRepo, never()).deleteById(anyString());
    }

    @Test
    @DisplayName("Should handle MongoDB delete failure")
    void cancelTimedOutTransactions_MongoDeleteFailure_ShouldThrowException() {
        // Arrange
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));
        doThrow(new RuntimeException("MongoDB delete error")).when(pendingRepo).deleteById("txn123");

        // Act & Assert
        assertThatThrownBy(() -> timeoutService.cancelTimedOutTransactions())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("MongoDB delete error");

        // Verify save was called before delete failure
        verify(transactionRepo).save(any());
    }

    @Test
    @DisplayName("Should handle repository query failure")
    void cancelTimedOutTransactions_QueryFailure_ShouldThrowException() {
        // Arrange
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenThrow(new RuntimeException("Database query error"));

        // Act & Assert
        assertThatThrownBy(() -> timeoutService.cancelTimedOutTransactions())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Database query error");

        verify(transactionRepo, never()).save(any());
        verify(pendingRepo, never()).deleteById(anyString());
    }

    @Test
    @DisplayName("Should continue processing remaining transactions after one fails")
    void cancelTimedOutTransactions_PartialFailure_ShouldContinueProcessing() {
        // Arrange
        PendingTransaction txn1 = PendingTransaction.builder()
                .transactionId("txn1")
                .userId("user1")
                .amount(50000.0)
                .status("PROCESSING")
                .createdAt(Instant.now().minusSeconds(90))
                .build();

        PendingTransaction txn2 = PendingTransaction.builder()
                .transactionId("txn2")
                .userId("user2")
                .amount(75000.0)
                .status("PROCESSING")
                .createdAt(Instant.now().minusSeconds(90))
                .build();

        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Arrays.asList(txn1, txn2));

        // First transaction succeeds, second fails
        doNothing().when(pendingRepo).deleteById("txn1");
        doThrow(new RuntimeException("Delete error")).when(pendingRepo).deleteById("txn2");

        // Act & Assert
        assertThatThrownBy(() -> timeoutService.cancelTimedOutTransactions())
                .isInstanceOf(RuntimeException.class);

        // Verify both were attempted to be saved
        verify(transactionRepo, times(2)).save(any());
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    @DisplayName("Should handle transaction at exact timeout boundary")
    void cancelTimedOutTransactions_ExactBoundary_ShouldCancel() {
        // Arrange
        PendingTransaction boundaryTxn = PendingTransaction.builder()
                .transactionId("boundary-txn")
                .userId("user123")
                .amount(60000.0)
                .status("PROCESSING")
                .createdAt(Instant.now().minusSeconds(60))
                .build();

        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(boundaryTxn));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        verify(transactionRepo).save(any());
        verify(pendingRepo).deleteById("boundary-txn");
    }

    @Test
    @DisplayName("Should handle transaction with null userId")
    void cancelTimedOutTransactions_NullUserId_ShouldCancel() {
        // Arrange
        timedOutTransaction.setUserId(null);
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        ArgumentCaptor<TransactionEntity> entityCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(transactionRepo).save(entityCaptor.capture());

        TransactionEntity savedEntity = entityCaptor.getValue();
        assertThat(savedEntity.getUserId()).isNull();
        assertThat(savedEntity.getStatus()).isEqualTo("CANCELLED");
    }

    @Test
    @DisplayName("Should handle transaction with zero amount")
    void cancelTimedOutTransactions_ZeroAmount_ShouldCancel() {
        // Arrange
        timedOutTransaction.setAmount(0.0);
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        ArgumentCaptor<TransactionEntity> entityCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(transactionRepo).save(entityCaptor.capture());

        TransactionEntity savedEntity = entityCaptor.getValue();
        assertThat(savedEntity.getAmount()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("Should handle transaction with negative amount")
    void cancelTimedOutTransactions_NegativeAmount_ShouldCancel() {
        // Arrange
        timedOutTransaction.setAmount(-1000.0);
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        ArgumentCaptor<TransactionEntity> entityCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(transactionRepo).save(entityCaptor.capture());

        TransactionEntity savedEntity = entityCaptor.getValue();
        assertThat(savedEntity.getAmount()).isEqualTo(-1000.0);
    }

    @Test
    @DisplayName("Should handle very old transactions")
    void cancelTimedOutTransactions_VeryOldTransaction_ShouldCancel() {
        // Arrange
        timedOutTransaction.setCreatedAt(Instant.now().minusSeconds(86400)); // 24 hours old
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        verify(transactionRepo).save(any());
        verify(pendingRepo).deleteById("txn123");
    }

    @Test
    @DisplayName("Should handle transaction with special characters in ID")
    void cancelTimedOutTransactions_SpecialCharactersId_ShouldCancel() {
        // Arrange
        timedOutTransaction.setTransactionId("txn-123@special#id");
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        verify(pendingRepo).deleteById("txn-123@special#id");
    }

    @Test
    @DisplayName("Should handle very large number of timed-out transactions")
    void cancelTimedOutTransactions_LargeVolume_ShouldCancelAll() {
        // Arrange
        List<PendingTransaction> largeList = new java.util.ArrayList<>();
        for (int i = 0; i < 100; i++) {
            largeList.add(PendingTransaction.builder()
                    .transactionId("txn" + i)
                    .userId("user" + i)
                    .amount(50000.0 + i)
                    .status("PROCESSING")
                    .createdAt(Instant.now().minusSeconds(90))
                    .build());
        }

        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(largeList);

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        verify(transactionRepo, times(100)).save(any());
        verify(pendingRepo, times(100)).deleteById(anyString());
    }

    // ==================== SCHEDULED EXECUTION TESTS ====================

    @Test
    @DisplayName("Should be annotated with @Scheduled for automatic execution")
    void cancelTimedOutTransactions_ScheduledAnnotation_ShouldExist() throws NoSuchMethodException {
        // Assert
        var method = TransactionTimeoutService.class.getMethod("cancelTimedOutTransactions");
        var scheduledAnnotation = method.getAnnotation(org.springframework.scheduling.annotation.Scheduled.class);

        assertThat(scheduledAnnotation).isNotNull();
        assertThat(scheduledAnnotation.fixedRate()).isEqualTo(10000); // 10 seconds
    }

    @Test
    @DisplayName("Should handle concurrent executions gracefully")
    void cancelTimedOutTransactions_ConcurrentExecution_ShouldHandleGracefully() {
        // Arrange
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act - Simulate concurrent executions
        timeoutService.cancelTimedOutTransactions();
        timeoutService.cancelTimedOutTransactions();

        // Assert - Each execution should query independently
        verify(pendingRepo, times(2)).findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class));
    }

    @Test
    @DisplayName("Should handle null createdAt timestamp")
    void cancelTimedOutTransactions_NullCreatedAt_ShouldCancel() {
        // Arrange
        timedOutTransaction.setCreatedAt(null);
        when(pendingRepo.findByStatusAndCreatedAtBefore(eq("PROCESSING"), any(Instant.class)))
                .thenReturn(Collections.singletonList(timedOutTransaction));

        // Act
        timeoutService.cancelTimedOutTransactions();

        // Assert
        ArgumentCaptor<TransactionEntity> entityCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(transactionRepo).save(entityCaptor.capture());

        TransactionEntity savedEntity = entityCaptor.getValue();
        assertThat(savedEntity.getCreatedAt()).isNull();
        assertThat(savedEntity.getStatus()).isEqualTo("CANCELLED");
    }
}

// Made with Bob
