package com.buk.hackathon.service;


import lombok.RequiredArgsConstructor;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Service;

import com.buk.hackathon.dto.CreateTransactionRequest;
import com.buk.hackathon.entity.PendingTransaction;
import com.buk.hackathon.entity.TransactionEntity;
import com.buk.hackathon.kafka.event.TransactionEvent;
import com.buk.hackathon.kafka.producer.TransactionProducer;
import com.buk.hackathon.repository.PendingTransactionRepository;
import com.buk.hackathon.repository.TransactionRepository;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TransactionService {

    private final PendingTransactionRepository pendingRepo;
    private final TransactionProducer producer;
    private final TransactionRepository transactionRepository;

    private static final double THRESHOLD = 50000;

    public String createTransaction(CreateTransactionRequest request) {

        String transactionId = UUID.randomUUID().toString();

        String status = request.getAmount() > THRESHOLD ? "PENDING" : "COMPLETED";

        // Save to Mongo (CQRS write side for pending)
        PendingTransaction txn = PendingTransaction.builder()
                .transactionId(transactionId)
                .userId(request.getUserId())
                .amount(request.getAmount())
                .status(status)
                .createdAt(LocalDateTime.now())
                .offers(null)
                .build();

        pendingRepo.save(txn);

        // Send Kafka event if high-value
        if ("PENDING".equals(status)) {
            TransactionEvent event = TransactionEvent.builder()
                    .transactionId(transactionId)
                    .userId(request.getUserId())
                    .amount(request.getAmount())
                    .status(status)
                    .build();

            producer.sendTransactionEvent(event);
        }

        return transactionId;
    }
    
    public Object getOffers(String transactionId) {

        PendingTransaction txn = pendingRepo.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found"));

        return txn.getOffers();
    }
    
    public void selectOffer(String transactionId, String offerId) {

        PendingTransaction txn = pendingRepo.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found"));

        boolean valid = txn.getOffers().stream()
                .anyMatch(o -> o.getOfferId().equals(offerId));

        if (!valid) {
            throw new RuntimeException("Invalid offer");
        }

        txn.setStatus("PROCESSING");
        pendingRepo.save(txn);

        // Save final state in PostgreSQL
        TransactionEntity entity = TransactionEntity.builder()
                .transactionId(transactionId)
                .userId(txn.getUserId())
                .amount(txn.getAmount())
                .status("COMPLETED")
                .selectedOffer(offerId)
                .createdAt(txn.getCreatedAt())
                .build();

		transactionRepository.save(entity);

        // Remove from Mongo (optional cleanup)
        pendingRepo.deleteById(transactionId);
    }
}