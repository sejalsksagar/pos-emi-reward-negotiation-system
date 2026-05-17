package com.pos.service;

import com.pos.dto.CreateTransactionRequest;
import com.pos.entity.Offer;
import com.pos.entity.PendingTransaction;
import com.pos.entity.TransactionEntity;
import com.pos.kafka.event.TransactionEvent;
import com.pos.kafka.producer.TransactionProducer;
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
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("TransactionService Tests")
class TransactionServiceTest {

    @Mock
    private PendingTransactionRepository pendingRepo;

    @Mock
    private TransactionProducer producer;

    @Mock
    private TransactionRepository transactionRepository;

    @InjectMocks
    private TransactionService transactionService;

    private CreateTransactionRequest request;
    private static final double THRESHOLD = 50000;

    @BeforeEach
    void setUp() {
        request = new CreateTransactionRequest();
        request.setUserId("user123");
    }

    // ==================== CREATE TRANSACTION TESTS ====================

    @Test
    @DisplayName("Should create high-value transaction with PENDING status and publish Kafka event")
    void createTransaction_HighValue_ShouldPublishKafkaEvent() {
        // Arrange
        request.setAmount(60000.0);

        // Act
        String transactionId = transactionService.createTransaction(request);

        // Assert
        assertThat(transactionId).isNotNull();

        // Verify pending transaction saved
        ArgumentCaptor<PendingTransaction> txnCaptor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(txnCaptor.capture());

        PendingTransaction savedTxn = txnCaptor.getValue();
        assertThat(savedTxn.getTransactionId()).isEqualTo(transactionId);
        assertThat(savedTxn.getUserId()).isEqualTo("user123");
        assertThat(savedTxn.getAmount()).isEqualTo(60000.0);
        assertThat(savedTxn.getStatus()).isEqualTo("PENDING");
        assertThat(savedTxn.getCreatedAt()).isNotNull();
        assertThat(savedTxn.getOffers()).isNull();

        // Verify Kafka event published
        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(producer).sendTransactionEvent(eventCaptor.capture());

        TransactionEvent event = eventCaptor.getValue();
        assertThat(event.getTransactionId()).isEqualTo(transactionId);
        assertThat(event.getUserId()).isEqualTo("user123");
        assertThat(event.getAmount()).isEqualTo(60000.0);
        assertThat(event.getStatus()).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("Should create low-value transaction with COMPLETED status without Kafka event")
    void createTransaction_LowValue_ShouldNotPublishKafkaEvent() {
        // Arrange
        request.setAmount(30000.0);

        // Act
        String transactionId = transactionService.createTransaction(request);

        // Assert
        assertThat(transactionId).isNotNull();

        // Verify pending transaction saved with COMPLETED status
        ArgumentCaptor<PendingTransaction> txnCaptor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(txnCaptor.capture());

        PendingTransaction savedTxn = txnCaptor.getValue();
        assertThat(savedTxn.getStatus()).isEqualTo("COMPLETED");

        // Verify NO Kafka event published
        verify(producer, never()).sendTransactionEvent(any());
    }

    @Test
    @DisplayName("Should create transaction at exact threshold boundary")
    void createTransaction_ExactThreshold_ShouldNotPublishKafkaEvent() {
        // Arrange
        request.setAmount(THRESHOLD);

        // Act
        String transactionId = transactionService.createTransaction(request);

        // Assert
        ArgumentCaptor<PendingTransaction> txnCaptor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(txnCaptor.capture());

        PendingTransaction savedTxn = txnCaptor.getValue();
        assertThat(savedTxn.getStatus()).isEqualTo("COMPLETED");
        verify(producer, never()).sendTransactionEvent(any());
    }

    @Test
    @DisplayName("Should create transaction just above threshold")
    void createTransaction_JustAboveThreshold_ShouldPublishKafkaEvent() {
        // Arrange
        request.setAmount(THRESHOLD + 0.01);

        // Act
        String transactionId = transactionService.createTransaction(request);

        // Assert
        ArgumentCaptor<PendingTransaction> txnCaptor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(txnCaptor.capture());

        PendingTransaction savedTxn = txnCaptor.getValue();
        assertThat(savedTxn.getStatus()).isEqualTo("PENDING");
        verify(producer).sendTransactionEvent(any());
    }

    @Test
    @DisplayName("Should handle zero amount transaction")
    void createTransaction_ZeroAmount_ShouldCreateCompleted() {
        // Arrange
        request.setAmount(0.0);

        // Act
        String transactionId = transactionService.createTransaction(request);

        // Assert
        ArgumentCaptor<PendingTransaction> txnCaptor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(txnCaptor.capture());

        PendingTransaction savedTxn = txnCaptor.getValue();
        assertThat(savedTxn.getStatus()).isEqualTo("COMPLETED");
        verify(producer, never()).sendTransactionEvent(any());
    }

    @Test
    @DisplayName("Should handle negative amount transaction")
    void createTransaction_NegativeAmount_ShouldCreateCompleted() {
        // Arrange
        request.setAmount(-1000.0);

        // Act
        String transactionId = transactionService.createTransaction(request);

        // Assert
        ArgumentCaptor<PendingTransaction> txnCaptor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(txnCaptor.capture());

        PendingTransaction savedTxn = txnCaptor.getValue();
        assertThat(savedTxn.getStatus()).isEqualTo("COMPLETED");
        verify(producer, never()).sendTransactionEvent(any());
    }

    @Test
    @DisplayName("Should handle repository failure during save")
    void createTransaction_RepositoryFailure_ShouldThrowException() {
        // Arrange
        request.setAmount(60000.0);
        when(pendingRepo.save(any())).thenThrow(new RuntimeException("Database error"));

        // Act & Assert
        assertThatThrownBy(() -> transactionService.createTransaction(request))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Database error");

        verify(producer, never()).sendTransactionEvent(any());
    }

    @Test
    @DisplayName("Should handle Kafka producer failure")
    void createTransaction_KafkaFailure_ShouldStillSaveTransaction() {
        // Arrange
        request.setAmount(60000.0);
        doThrow(new RuntimeException("Kafka error")).when(producer).sendTransactionEvent(any());

        // Act & Assert
        assertThatThrownBy(() -> transactionService.createTransaction(request))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Kafka error");

        // Verify transaction was saved before Kafka failure
        verify(pendingRepo).save(any());
    }

    // ==================== GET OFFERS TESTS ====================

    @Test
    @DisplayName("Should retrieve offers for existing transaction")
    void getOffers_ExistingTransaction_ShouldReturnOffers() {
        // Arrange
        String transactionId = "txn123";
        List<Offer> offers = Arrays.asList(
                Offer.builder().offerId("offer1").type("EMI").description("6 months EMI").build(),
                Offer.builder().offerId("offer2").type("CASHBACK").description("5% cashback").build()
        );

        PendingTransaction txn = PendingTransaction.builder()
                .transactionId(transactionId)
                .userId("user123")
                .amount(60000.0)
                .status("PROCESSING")
                .offers(offers)
                .createdAt(Instant.now())
                .build();

        when(pendingRepo.findById(transactionId)).thenReturn(Optional.of(txn));

        // Act
        Object result = transactionService.getOffers(transactionId);

        // Assert
        assertThat(result).isEqualTo(offers);
        verify(pendingRepo).findById(transactionId);
    }

    @Test
    @DisplayName("Should return null offers for transaction without offers")
    void getOffers_NoOffers_ShouldReturnNull() {
        // Arrange
        String transactionId = "txn123";
        PendingTransaction txn = PendingTransaction.builder()
                .transactionId(transactionId)
                .userId("user123")
                .amount(60000.0)
                .status("PENDING")
                .offers(null)
                .createdAt(Instant.now())
                .build();

        when(pendingRepo.findById(transactionId)).thenReturn(Optional.of(txn));

        // Act
        Object result = transactionService.getOffers(transactionId);

        // Assert
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Should throw exception when transaction not found")
    void getOffers_TransactionNotFound_ShouldThrowException() {
        // Arrange
        String transactionId = "nonexistent";
        when(pendingRepo.findById(transactionId)).thenReturn(Optional.empty());

        // Act & Assert
        assertThatThrownBy(() -> transactionService.getOffers(transactionId))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Transaction not found");
    }

    // ==================== SELECT OFFER TESTS ====================

    @Test
    @DisplayName("Should successfully select valid offer")
    void selectOffer_ValidOffer_ShouldCompleteTransaction() {
        // Arrange
        String transactionId = "txn123";
        String offerId = "offer1";
        Instant createdAt = Instant.now();

        List<Offer> offers = Arrays.asList(
                Offer.builder().offerId("offer1").type("EMI").description("6 months EMI").build(),
                Offer.builder().offerId("offer2").type("CASHBACK").description("5% cashback").build()
        );

        PendingTransaction txn = PendingTransaction.builder()
                .transactionId(transactionId)
                .userId("user123")
                .amount(60000.0)
                .status("PROCESSING")
                .offers(offers)
                .createdAt(createdAt)
                .build();

        when(pendingRepo.findById(transactionId)).thenReturn(Optional.of(txn));

        // Act
        transactionService.selectOffer(transactionId, offerId);

        // Assert
        // Verify pending transaction updated to PROCESSING
        ArgumentCaptor<PendingTransaction> pendingCaptor = ArgumentCaptor.forClass(PendingTransaction.class);
        verify(pendingRepo).save(pendingCaptor.capture());
        assertThat(pendingCaptor.getValue().getStatus()).isEqualTo("PROCESSING");

        // Verify final transaction saved to PostgreSQL
        ArgumentCaptor<TransactionEntity> entityCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(transactionRepository).save(entityCaptor.capture());

        TransactionEntity savedEntity = entityCaptor.getValue();
        assertThat(savedEntity.getTransactionId()).isEqualTo(transactionId);
        assertThat(savedEntity.getUserId()).isEqualTo("user123");
        assertThat(savedEntity.getAmount()).isEqualTo(60000.0);
        assertThat(savedEntity.getStatus()).isEqualTo("COMPLETED");
        assertThat(savedEntity.getSelectedOffer()).isEqualTo(offerId);
        assertThat(savedEntity.getCreatedAt()).isEqualTo(createdAt);

        // Verify pending transaction deleted
        verify(pendingRepo).deleteById(transactionId);
    }

    @Test
    @DisplayName("Should throw exception for invalid offer")
    void selectOffer_InvalidOffer_ShouldThrowException() {
        // Arrange
        String transactionId = "txn123";
        String invalidOfferId = "invalidOffer";

        List<Offer> offers = Arrays.asList(
                Offer.builder().offerId("offer1").type("EMI").description("6 months EMI").build(),
                Offer.builder().offerId("offer2").type("CASHBACK").description("5% cashback").build()
        );

        PendingTransaction txn = PendingTransaction.builder()
                .transactionId(transactionId)
                .userId("user123")
                .amount(60000.0)
                .status("PROCESSING")
                .offers(offers)
                .createdAt(Instant.now())
                .build();

        when(pendingRepo.findById(transactionId)).thenReturn(Optional.of(txn));

        // Act & Assert
        assertThatThrownBy(() -> transactionService.selectOffer(transactionId, invalidOfferId))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Invalid offer");

        // Verify no saves or deletes occurred
        verify(pendingRepo, times(1)).findById(transactionId); // Only the initial find
        verify(pendingRepo, never()).save(any());
        verify(transactionRepository, never()).save(any());
        verify(pendingRepo, never()).deleteById(any());
    }

    @Test
    @DisplayName("Should throw exception when selecting offer for non-existent transaction")
    void selectOffer_TransactionNotFound_ShouldThrowException() {
        // Arrange
        String transactionId = "nonexistent";
        String offerId = "offer1";
        when(pendingRepo.findById(transactionId)).thenReturn(Optional.empty());

        // Act & Assert
        assertThatThrownBy(() -> transactionService.selectOffer(transactionId, offerId))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Transaction not found");

        verify(transactionRepository, never()).save(any());
        verify(pendingRepo, never()).deleteById(any());
    }

    @Test
    @DisplayName("Should throw exception when transaction has no offers")
    void selectOffer_NoOffers_ShouldThrowException() {
        // Arrange
        String transactionId = "txn123";
        String offerId = "offer1";

        PendingTransaction txn = PendingTransaction.builder()
                .transactionId(transactionId)
                .userId("user123")
                .amount(60000.0)
                .status("PENDING")
                .offers(null)
                .createdAt(Instant.now())
                .build();

        when(pendingRepo.findById(transactionId)).thenReturn(Optional.of(txn));

        // Act & Assert
        assertThatThrownBy(() -> transactionService.selectOffer(transactionId, offerId))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should handle PostgreSQL save failure during offer selection")
    void selectOffer_PostgresFailure_ShouldThrowException() {
        // Arrange
        String transactionId = "txn123";
        String offerId = "offer1";

        List<Offer> offers = Arrays.asList(
                Offer.builder().offerId("offer1").type("EMI").description("6 months EMI").build()
        );

        PendingTransaction txn = PendingTransaction.builder()
                .transactionId(transactionId)
                .userId("user123")
                .amount(60000.0)
                .status("PROCESSING")
                .offers(offers)
                .createdAt(Instant.now())
                .build();

        when(pendingRepo.findById(transactionId)).thenReturn(Optional.of(txn));
        when(transactionRepository.save(any())).thenThrow(new RuntimeException("PostgreSQL error"));

        // Act & Assert
        assertThatThrownBy(() -> transactionService.selectOffer(transactionId, offerId))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("PostgreSQL error");

        // Verify pending transaction was updated but not deleted
        verify(pendingRepo).save(any());
        verify(pendingRepo, never()).deleteById(any());
    }

    @Test
    @DisplayName("Should handle MongoDB delete failure after successful completion")
    void selectOffer_MongoDeleteFailure_ShouldThrowException() {
        // Arrange
        String transactionId = "txn123";
        String offerId = "offer1";

        List<Offer> offers = Arrays.asList(
                Offer.builder().offerId("offer1").type("EMI").description("6 months EMI").build()
        );

        PendingTransaction txn = PendingTransaction.builder()
                .transactionId(transactionId)
                .userId("user123")
                .amount(60000.0)
                .status("PROCESSING")
                .offers(offers)
                .createdAt(Instant.now())
                .build();

        when(pendingRepo.findById(transactionId)).thenReturn(Optional.of(txn));
        doThrow(new RuntimeException("MongoDB delete error")).when(pendingRepo).deleteById(transactionId);

        // Act & Assert
        assertThatThrownBy(() -> transactionService.selectOffer(transactionId, offerId))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("MongoDB delete error");

        // Verify both saves occurred before delete failure
        verify(pendingRepo).save(any());
        verify(transactionRepository).save(any());
    }
}

// Made with Bob
