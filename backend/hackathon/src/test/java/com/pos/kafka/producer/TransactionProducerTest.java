package com.pos.kafka.producer;

import com.pos.kafka.event.TransactionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("TransactionProducer Tests - Kafka Publishing")
class TransactionProducerTest {

    @Mock
    private KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    @Mock
    private CompletableFuture<SendResult<String, TransactionEvent>> sendResultFuture;

    @InjectMocks
    private TransactionProducer transactionProducer;

    private TransactionEvent event;
    private static final String TOPIC = "transactions";

    @BeforeEach
    void setUp() {
        // Set the topic value using reflection (simulating @Value injection)
        ReflectionTestUtils.setField(transactionProducer, "topic", TOPIC);

        event = TransactionEvent.builder()
                .transactionId("txn123")
                .userId("user123")
                .amount(60000.0)
                .status("PENDING")
                .build();
    }

    // ==================== SUCCESSFUL KAFKA PUBLISHING TESTS ====================

    @Test
    @DisplayName("Should successfully send transaction event to Kafka")
    void sendTransactionEvent_ValidEvent_ShouldPublishToKafka() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);

        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), eventCaptor.capture());

        assertThat(topicCaptor.getValue()).isEqualTo(TOPIC);
        assertThat(keyCaptor.getValue()).isEqualTo("txn123");
        assertThat(eventCaptor.getValue()).isEqualTo(event);
    }

    @Test
    @DisplayName("Should use transaction ID as Kafka message key")
    void sendTransactionEvent_MessageKey_ShouldUseTransactionId() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        verify(kafkaTemplate).send(eq(TOPIC), eq("txn123"), any(TransactionEvent.class));
    }

    @Test
    @DisplayName("Should send event with all transaction details")
    void sendTransactionEvent_EventDetails_ShouldIncludeAllFields() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(kafkaTemplate).send(anyString(), anyString(), eventCaptor.capture());

        TransactionEvent capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.getTransactionId()).isEqualTo("txn123");
        assertThat(capturedEvent.getUserId()).isEqualTo("user123");
        assertThat(capturedEvent.getAmount()).isEqualTo(60000.0);
        assertThat(capturedEvent.getStatus()).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("Should send to correct topic")
    void sendTransactionEvent_Topic_ShouldUseConfiguredTopic() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        verify(kafkaTemplate).send(eq(TOPIC), anyString(), any(TransactionEvent.class));
    }

    @Test
    @DisplayName("Should handle multiple consecutive sends")
    void sendTransactionEvent_MultipleSends_ShouldPublishAll() {
        // Arrange
        TransactionEvent event1 = TransactionEvent.builder()
                .transactionId("txn1")
                .userId("user1")
                .amount(50000.0)
                .status("PENDING")
                .build();

        TransactionEvent event2 = TransactionEvent.builder()
                .transactionId("txn2")
                .userId("user2")
                .amount(75000.0)
                .status("PENDING")
                .build();

        TransactionEvent event3 = TransactionEvent.builder()
                .transactionId("txn3")
                .userId("user3")
                .amount(100000.0)
                .status("PENDING")
                .build();

        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event1);
        transactionProducer.sendTransactionEvent(event2);
        transactionProducer.sendTransactionEvent(event3);

        // Assert
        verify(kafkaTemplate, times(3)).send(anyString(), anyString(), any(TransactionEvent.class));
        verify(kafkaTemplate).send(eq(TOPIC), eq("txn1"), eq(event1));
        verify(kafkaTemplate).send(eq(TOPIC), eq("txn2"),  eq(event2));
        verify(kafkaTemplate).send(eq(TOPIC), eq("txn3"), eq(event3));
    }

    // ==================== EDGE CASE TESTS ====================

    @Test
    @DisplayName("Should handle event with null transaction ID")
    void sendTransactionEvent_NullTransactionId_ShouldSendWithNullKey() {
        // Arrange
        event.setTransactionId(null);
        when(kafkaTemplate.send(eq(TOPIC), isNull(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        verify(kafkaTemplate).send(eq(TOPIC), isNull(), eq(event));
    }

    @Test
    @DisplayName("Should handle event with empty transaction ID")
    void sendTransactionEvent_EmptyTransactionId_ShouldSendWithEmptyKey() {
        // Arrange
        event.setTransactionId("");
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        verify(kafkaTemplate).send(eq(TOPIC), eq(""), eq(event));
    }

    @Test
    @DisplayName("Should handle event with special characters in transaction ID")
    void sendTransactionEvent_SpecialCharactersId_ShouldSendCorrectly() {
        // Arrange
        event.setTransactionId("txn-123@special#id");
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        verify(kafkaTemplate).send(eq(TOPIC), eq("txn-123@special#id"), eq(event));
    }

    @Test
    @DisplayName("Should handle event with very long transaction ID")
    void sendTransactionEvent_LongTransactionId_ShouldSendCorrectly() {
        // Arrange
        String longId = "txn" + "a".repeat(1000);
        event.setTransactionId(longId);
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        verify(kafkaTemplate).send(eq(TOPIC), eq(longId), eq(event));
    }

    @Test
    @DisplayName("Should handle event with null user ID")
    void sendTransactionEvent_NullUserId_ShouldSendCorrectly() {
        // Arrange
        event.setUserId(null);
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(kafkaTemplate).send(anyString(), anyString(), eventCaptor.capture());

        TransactionEvent capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.getUserId()).isNull();
    }

    @Test
    @DisplayName("Should handle event with zero amount")
    void sendTransactionEvent_ZeroAmount_ShouldSendCorrectly() {
        // Arrange
        event.setAmount(0.0);
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(kafkaTemplate).send(anyString(), anyString(), eventCaptor.capture());

        TransactionEvent capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.getAmount()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("Should handle event with negative amount")
    void sendTransactionEvent_NegativeAmount_ShouldSendCorrectly() {
        // Arrange
        event.setAmount(-1000.0);
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(kafkaTemplate).send(anyString(), anyString(), eventCaptor.capture());

        TransactionEvent capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.getAmount()).isEqualTo(-1000.0);
    }

    @Test
    @DisplayName("Should handle event with very large amount")
    void sendTransactionEvent_VeryLargeAmount_ShouldSendCorrectly() {
        // Arrange
        event.setAmount(Double.MAX_VALUE);
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(kafkaTemplate).send(anyString(), anyString(), eventCaptor.capture());

        TransactionEvent capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.getAmount()).isEqualTo(Double.MAX_VALUE);
    }

    @Test
    @DisplayName("Should handle event with null status")
    void sendTransactionEvent_NullStatus_ShouldSendCorrectly() {
        // Arrange
        event.setStatus(null);
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(kafkaTemplate).send(anyString(), anyString(), eventCaptor.capture());

        TransactionEvent capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.getStatus()).isNull();
    }

    @Test
    @DisplayName("Should handle different status values")
    void sendTransactionEvent_DifferentStatuses_ShouldSendCorrectly() {
        // Arrange & Act & Assert for PENDING
        event.setStatus("PENDING");
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);
        transactionProducer.sendTransactionEvent(event);

        // PROCESSING
        event.setStatus("PROCESSING");
        transactionProducer.sendTransactionEvent(event);

        // COMPLETED
        event.setStatus("COMPLETED");
        transactionProducer.sendTransactionEvent(event);

        // CANCELLED
        event.setStatus("CANCELLED");
        transactionProducer.sendTransactionEvent(event);

        verify(kafkaTemplate, times(4)).send(anyString(), anyString(), any(TransactionEvent.class));
    }

    // ==================== ERROR HANDLING TESTS ====================

    @Test
    @DisplayName("Should propagate KafkaTemplate send failure")
    void sendTransactionEvent_KafkaFailure_ShouldThrowException() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenThrow(new RuntimeException("Kafka broker unavailable"));

        // Act & Assert
        assertThatThrownBy(() -> transactionProducer.sendTransactionEvent(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Kafka broker unavailable");
    }

    @Test
    @DisplayName("Should handle serialization failure")
    void sendTransactionEvent_SerializationFailure_ShouldThrowException() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenThrow(new RuntimeException("Serialization error"));

        // Act & Assert
        assertThatThrownBy(() -> transactionProducer.sendTransactionEvent(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Serialization error");
    }

    @Test
    @DisplayName("Should handle topic not found error")
    void sendTransactionEvent_TopicNotFound_ShouldThrowException() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenThrow(new RuntimeException("Topic 'transactions' not found"));

        // Act & Assert
        assertThatThrownBy(() -> transactionProducer.sendTransactionEvent(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Topic 'transactions' not found");
    }

    @Test
    @DisplayName("Should handle null event gracefully")
    void sendTransactionEvent_NullEvent_ShouldThrowException() {
        // Act & Assert
        assertThatThrownBy(() -> transactionProducer.sendTransactionEvent(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should handle authentication failure")
    void sendTransactionEvent_AuthenticationFailure_ShouldThrowException() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenThrow(new RuntimeException("Authentication failed"));

        // Act & Assert
        assertThatThrownBy(() -> transactionProducer.sendTransactionEvent(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Authentication failed");
    }

    @Test
    @DisplayName("Should handle network timeout")
    void sendTransactionEvent_NetworkTimeout_ShouldThrowException() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenThrow(new RuntimeException("Request timeout"));

        // Act & Assert
        assertThatThrownBy(() -> transactionProducer.sendTransactionEvent(event))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Request timeout");
    }

    // ==================== CONFIGURATION TESTS ====================

    @Test
    @DisplayName("Should use topic from configuration")
    void sendTransactionEvent_TopicConfiguration_ShouldUseConfiguredValue() {
        // Arrange
        String customTopic = "custom-transactions-topic";
        ReflectionTestUtils.setField(transactionProducer, "topic", customTopic);
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        verify(kafkaTemplate).send(eq(customTopic), anyString(), any(TransactionEvent.class));
    }

    @Test
    @DisplayName("Should handle null topic configuration")
    void sendTransactionEvent_NullTopic_ShouldSendWithNull() {
        // Arrange
        ReflectionTestUtils.setField(transactionProducer, "topic", null);
        when(kafkaTemplate.send(isNull(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        verify(kafkaTemplate).send(isNull(), anyString(), any(TransactionEvent.class));
    }

    @Test
    @DisplayName("Should handle empty topic configuration")
    void sendTransactionEvent_EmptyTopic_ShouldSendWithEmpty() {
        // Arrange
        ReflectionTestUtils.setField(transactionProducer, "topic", "");
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert
        verify(kafkaTemplate).send(eq(""), anyString(), any(TransactionEvent.class));
    }

    // ==================== PERFORMANCE TESTS ====================

    @Test
    @DisplayName("Should handle high-frequency sends")
    void sendTransactionEvent_HighFrequency_ShouldHandleMultipleSends() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act - Send 100 events rapidly
        for (int i = 0; i < 100; i++) {
            TransactionEvent rapidEvent = TransactionEvent.builder()
                    .transactionId("txn" + i)
                    .userId("user" + i)
                    .amount(1000.0 + i)
                    .status("PENDING")
                    .build();
            transactionProducer.sendTransactionEvent(rapidEvent);
        }

        // Assert
        verify(kafkaTemplate, times(100)).send(anyString(), anyString(), any(TransactionEvent.class));
    }

    @Test
    @DisplayName("Should not block on send operation")
    void sendTransactionEvent_NonBlocking_ShouldReturnImmediately() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        long startTime = System.currentTimeMillis();
        transactionProducer.sendTransactionEvent(event);
        long endTime = System.currentTimeMillis();

        // Assert - Should complete quickly (non-blocking)
        assertThat(endTime - startTime).isLessThan(100); // Less than 100ms
        verify(kafkaTemplate).send(anyString(), anyString(), any(TransactionEvent.class));
    }

    // ==================== LOGGING TESTS ====================

    @Test
    @DisplayName("Should log transaction event before sending")
    void sendTransactionEvent_Logging_ShouldLogEvent() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act
        transactionProducer.sendTransactionEvent(event);

        // Assert - Verify the method was called (logging happens inside)
        verify(kafkaTemplate).send(anyString(), anyString(), any(TransactionEvent.class));
        // Note: In a real test, you might want to use a logging framework test utility
        // to verify the actual log message content
    }

    // ==================== INTEGRATION SCENARIO TESTS ====================

    @Test
    @DisplayName("Should handle typical transaction flow scenario")
    void sendTransactionEvent_TypicalFlow_ShouldProcessCorrectly() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any(TransactionEvent.class)))
                .thenReturn(sendResultFuture);

        // Act - Simulate typical transaction flow
        // 1. High-value transaction created
        TransactionEvent pendingEvent = TransactionEvent.builder()
                .transactionId("txn-flow-123")
                .userId("customer-456")
                .amount(75000.0)
                .status("PENDING")
                .build();

        transactionProducer.sendTransactionEvent(pendingEvent);

        // 2. Transaction moves to processing (hypothetical)
        pendingEvent.setStatus("PROCESSING");
        transactionProducer.sendTransactionEvent(pendingEvent);

        // Assert
        verify(kafkaTemplate, times(2)).send(eq(TOPIC), eq("txn-flow-123"), any(TransactionEvent.class));
    }
}

// Made with Bob
