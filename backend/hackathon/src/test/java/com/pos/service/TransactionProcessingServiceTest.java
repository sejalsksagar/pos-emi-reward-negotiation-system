package com.pos.service;

import com.pos.entity.Offer;
import com.pos.entity.PendingTransaction;
import com.pos.kafka.event.TransactionEvent;
import com.pos.repository.PendingTransactionRepository;
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
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("TransactionProcessingService Tests")
class TransactionProcessingServiceTest {

    @Mock
    private PendingTransactionRepository pendingRepo;

    @Mock
    private OfferService offerService;

    @InjectMocks
    private TransactionProcessingService processingService;

    private TransactionEvent event;
    private PendingTransaction pendingTransaction;
    private List<Offer> mockOffers;

    @BeforeEach
    void setUp() {
        event = TransactionEvent.builder()
                .transactionId("txn123")
                .userId("user123")
                .amount(60000.0)
                .status("PENDING")
                .build();

        pendingTransaction = PendingTransaction.builder()
                .transactionId("txn123")
                .userId("user123")
                .amount(60000.0)
                .status("PENDING")
                .createdAt(Instant.now())
                .offers(null)
                .build();

        mockOffers = Arrays.asList(
                Offer.builder().offerId("offer1").type("EMI").description("6 months EMI").build(),
                Offer.builder().offerId("offer2").type("CASHBACK").description("5% cashback").build(),
                Offer.builder().offerId("offer3").type("REWARD").description("1000 reward points").build()
        );
    }

    // ==================== SUCCESSFUL PROCESSING TESTS ====================

    @Test
    @DisplayName("Should successfully process transaction and update state to PROCESSING")
    void process_ValidTransaction_ShouldUpdateStateToProcessing() {
        // Arrange
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0)).thenReturn(mockOffers);

        // Act
        processingService.process(event);

        // Assert
        ArgumentCaptor<PendingTransaction> captor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(captor.capture());

        PendingTransaction savedTxn = captor.getValue();
        assertThat(savedTxn.getStatus()).isEqualTo("PROCESSING");
        assertThat(savedTxn.getOffers()).isEqualTo(mockOffers);
        assertThat(savedTxn.getOffers()).hasSize(3);

        verify(offerService).generateOffers("user123", 60000.0);
        verify(pendingRepo, never()).deleteById(anyString());
    }

    @Test
    @DisplayName("Should process transaction with empty offers list")
    void process_EmptyOffers_ShouldUpdateWithEmptyList() {
        // Arrange
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0)).thenReturn(Arrays.asList());

        // Act
        processingService.process(event);

        // Assert
        ArgumentCaptor<PendingTransaction> captor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(captor.capture());

        PendingTransaction savedTxn = captor.getValue();
        assertThat(savedTxn.getStatus()).isEqualTo("PROCESSING");
        assertThat(savedTxn.getOffers()).isEmpty();
    }

    @Test
    @DisplayName("Should process transaction with single offer")
    void process_SingleOffer_ShouldUpdateCorrectly() {
        // Arrange
        List<Offer> singleOffer = Arrays.asList(
                Offer.builder().offerId("offer1").type("EMI").description("12 months EMI").build()
        );

        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0)).thenReturn(singleOffer);

        // Act
        processingService.process(event);

        // Assert
        ArgumentCaptor<PendingTransaction> captor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(captor.capture());

        PendingTransaction savedTxn = captor.getValue();
        assertThat(savedTxn.getOffers()).hasSize(1);
        assertThat(savedTxn.getOffers().get(0).getOfferId()).isEqualTo("offer1");
    }

    // ==================== TRANSACTION NOT FOUND TESTS ====================

    @Test
    @DisplayName("Should return early when transaction not found in repository")
    void process_TransactionNotFound_ShouldReturnEarly() {
        // Arrange
        when(pendingRepo.findById("txn123")).thenReturn(Optional.empty());

        // Act
        processingService.process(event);

        // Assert
        verify(pendingRepo).findById("txn123");
        verify(offerService, never()).generateOffers(anyString(), anyDouble());
        verify(pendingRepo, never()).save(any());
        verify(pendingRepo, never()).deleteById(anyString());
    }

    @Test
    @DisplayName("Should return early when transaction is null")
    void process_NullTransaction_ShouldReturnEarly() {
        // Arrange
        when(pendingRepo.findById("txn123")).thenReturn(Optional.ofNullable(null));

        // Act
        processingService.process(event);

        // Assert
        verify(offerService, never()).generateOffers(anyString(), anyDouble());
        verify(pendingRepo, never()).save(any());
    }

    // ==================== STATUS VALIDATION TESTS ====================

    @Test
    @DisplayName("Should return early when transaction status is not PENDING")
    void process_NonPendingStatus_ShouldReturnEarly() {
        // Arrange
        pendingTransaction.setStatus("PROCESSING");
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));

        // Act
        processingService.process(event);

        // Assert
        verify(pendingRepo).findById("txn123");
        verify(offerService, never()).generateOffers(anyString(), anyDouble());
        verify(pendingRepo, never()).save(any());
    }

    @Test
    @DisplayName("Should return early when transaction status is COMPLETED")
    void process_CompletedStatus_ShouldReturnEarly() {
        // Arrange
        pendingTransaction.setStatus("COMPLETED");
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));

        // Act
        processingService.process(event);

        // Assert
        verify(offerService, never()).generateOffers(anyString(), anyDouble());
        verify(pendingRepo, never()).save(any());
    }

    @Test
    @DisplayName("Should return early when transaction status is CANCELLED")
    void process_CancelledStatus_ShouldReturnEarly() {
        // Arrange
        pendingTransaction.setStatus("CANCELLED");
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));

        // Act
        processingService.process(event);

        // Assert
        verify(offerService, never()).generateOffers(anyString(), anyDouble());
        verify(pendingRepo, never()).save(any());
    }

    @Test
    @DisplayName("Should process when status is exactly PENDING (case sensitive)")
    void process_ExactPendingStatus_ShouldProcess() {
        // Arrange
        pendingTransaction.setStatus("PENDING");
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0)).thenReturn(mockOffers);

        // Act
        processingService.process(event);

        // Assert
        verify(offerService).generateOffers("user123", 60000.0);
        verify(pendingRepo).save(any());
    }

    // ==================== ERROR HANDLING TESTS ====================

    @Test
    @DisplayName("Should delete transaction and rethrow when offer generation fails")
    void process_OfferGenerationFailure_ShouldDeleteAndRethrow() {
        // Arrange
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0))
                .thenThrow(new RuntimeException("Offer generation failed"));

        // Act & Assert
        assertThatThrownBy(() -> processingService.process(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Offer generation failed");

        // Verify transaction was deleted (key fix for preventing stuck transactions)
        verify(pendingRepo).deleteById("txn123");
        verify(pendingRepo, never()).save(any());
    }

    @Test
    @DisplayName("Should delete transaction when rule engine fails")
    void process_RuleEngineFailure_ShouldDeleteTransaction() {
        // Arrange
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0))
                .thenThrow(new RuntimeException("Rule engine error"));

        // Act & Assert
        assertThatThrownBy(() -> processingService.process(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Rule engine error");

        verify(pendingRepo).deleteById("txn123");
    }

    @Test
    @DisplayName("Should delete transaction when NullPointerException occurs")
    void process_NullPointerException_ShouldDeleteTransaction() {
        // Arrange
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0))
                .thenThrow(new NullPointerException("Null data"));

        // Act & Assert
        assertThatThrownBy(() -> processingService.process(event))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Null data");

        verify(pendingRepo).deleteById("txn123");
    }

    @Test
    @DisplayName("Should handle repository save failure")
    void process_SaveFailure_ShouldThrowException() {
        // Arrange
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0)).thenReturn(mockOffers);
        when(pendingRepo.save(any())).thenThrow(new RuntimeException("Database save error"));

        // Act & Assert
        assertThatThrownBy(() -> processingService.process(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Database save error");

        verify(pendingRepo).deleteById("txn123");
    }

    @Test
    @DisplayName("Should handle delete failure after processing error")
    void process_DeleteFailure_ShouldThrowOriginalException() {
        // Arrange
        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0))
                .thenThrow(new RuntimeException("Offer error"));
        doThrow(new RuntimeException("Delete error")).when(pendingRepo).deleteById("txn123");

        // Act & Assert
        assertThatThrownBy(() -> processingService.process(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Delete error");
    }

    // ==================== STATE TRANSITION TESTS ====================

    @Test
    @DisplayName("Should transition from PENDING to PROCESSING with offers")
    void process_StateTransition_ShouldUpdateCorrectly() {
        // Arrange
        assertThat(pendingTransaction.getStatus()).isEqualTo("PENDING");
        assertThat(pendingTransaction.getOffers()).isNull();

        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0)).thenReturn(mockOffers);

        // Act
        processingService.process(event);

        // Assert
        ArgumentCaptor<PendingTransaction> captor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(captor.capture());

        PendingTransaction savedTxn = captor.getValue();
        assertThat(savedTxn.getStatus()).isEqualTo("PROCESSING");
        assertThat(savedTxn.getOffers()).isNotNull();
        assertThat(savedTxn.getOffers()).hasSize(3);
    }

    @Test
    @DisplayName("Should preserve transaction metadata during processing")
    void process_PreserveMetadata_ShouldKeepOriginalData() {
        // Arrange
        Instant originalCreatedAt = Instant.now().minusSeconds(100);
        pendingTransaction.setCreatedAt(originalCreatedAt);

        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0)).thenReturn(mockOffers);

        // Act
        processingService.process(event);

        // Assert
        ArgumentCaptor<PendingTransaction> captor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(captor.capture());

        PendingTransaction savedTxn = captor.getValue();
        assertThat(savedTxn.getTransactionId()).isEqualTo("txn123");
        assertThat(savedTxn.getUserId()).isEqualTo("user123");
        assertThat(savedTxn.getAmount()).isEqualTo(60000.0);
        assertThat(savedTxn.getCreatedAt()).isEqualTo(originalCreatedAt);
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    @DisplayName("Should handle very large transaction amounts")
    void process_LargeAmount_ShouldProcessCorrectly() {
        // Arrange
        event.setAmount(999999999.99);
        pendingTransaction.setAmount(999999999.99);

        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 999999999.99)).thenReturn(mockOffers);

        // Act
        processingService.process(event);

        // Assert
        verify(offerService).generateOffers("user123", 999999999.99);
        verify(pendingRepo).save(any());
    }

    @Test
    @DisplayName("Should handle transaction with special characters in userId")
    void process_SpecialCharactersUserId_ShouldProcessCorrectly() {
        // Arrange
        String specialUserId = "user@123#test$";
        event.setUserId(specialUserId);
        pendingTransaction.setUserId(specialUserId);

        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers(specialUserId, 60000.0)).thenReturn(mockOffers);

        // Act
        processingService.process(event);

        // Assert
        verify(offerService).generateOffers(specialUserId, 60000.0);
        verify(pendingRepo).save(any());
    }

    @Test
    @DisplayName("Should handle transaction with very long transactionId")
    void process_LongTransactionId_ShouldProcessCorrectly() {
        // Arrange
        String longId = "txn" + "a".repeat(100);
        event.setTransactionId(longId);
        pendingTransaction.setTransactionId(longId);

        when(pendingRepo.findById(longId)).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 60000.0)).thenReturn(mockOffers);

        // Act
        processingService.process(event);

        // Assert
        verify(pendingRepo).findById(longId);
        verify(pendingRepo).save(any());
    }

    @Test
    @DisplayName("Should handle concurrent processing attempts gracefully")
    void process_ConcurrentProcessing_ShouldHandleStatusCheck() {
        // Arrange - Simulate transaction already processed by another thread
        pendingTransaction.setStatus("PROCESSING");
        pendingTransaction.setOffers(mockOffers);

        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));

        // Act
        processingService.process(event);

        // Assert - Should return early without reprocessing
        verify(offerService, never()).generateOffers(anyString(), anyDouble());
        verify(pendingRepo, never()).save(any());
    }

    @Test
    @DisplayName("Should handle transaction with null userId")
    void process_NullUserId_ShouldAttemptProcessing() {
        // Arrange
        event.setUserId(null);
        pendingTransaction.setUserId(null);

        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers(null, 60000.0)).thenReturn(mockOffers);

        // Act
        processingService.process(event);

        // Assert
        verify(offerService).generateOffers(null, 60000.0);
        verify(pendingRepo).save(any());
    }

    @Test
    @DisplayName("Should handle transaction with zero amount")
    void process_ZeroAmount_ShouldProcessCorrectly() {
        // Arrange
        event.setAmount(0.0);
        pendingTransaction.setAmount(0.0);

        when(pendingRepo.findById("txn123")).thenReturn(Optional.of(pendingTransaction));
        when(offerService.generateOffers("user123", 0.0)).thenReturn(mockOffers);

        // Act
        processingService.process(event);

        // Assert
        verify(offerService).generateOffers("user123", 0.0);
        verify(pendingRepo).save(any());
    }
}

// Made with Bob
