package com.pos.service;

import com.pos.entity.PendingTransaction;
import com.pos.entity.TransactionEntity;
import com.pos.repository.PendingTransactionRepository;
import com.pos.repository.TransactionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionTimeoutService {

    private final PendingTransactionRepository pendingRepo;
    private final TransactionRepository transactionRepo;

    // Runs every 10 seconds
    @Scheduled(fixedRate = 10000)
    public void cancelTimedOutTransactions() {

        Instant cutoff = Instant.now().minusSeconds(60);

        List<PendingTransaction> transactions =
                pendingRepo.findByStatusAndCreatedAtBefore("PROCESSING", cutoff);

        for (PendingTransaction txn : transactions) {

//            txn.setStatus("CANCELLED");
//            pendingRepo.save(txn);
//            
//            log.warn("Auto-cancelled txn {}", txn.getTransactionId());
            
         // Save final state in PostgreSQL
            TransactionEntity finalTxn = TransactionEntity.builder()
                    .transactionId(txn.getTransactionId())
                    .userId(txn.getUserId())
                    .amount(txn.getAmount())
                    .status("CANCELLED")
                    .createdAt(txn.getCreatedAt())
                    .build();

            transactionRepo.save(finalTxn);
            
            pendingRepo.deleteById(txn.getTransactionId());

            log.warn("Txn {} moved to CANCELLED (Postgres)", txn.getTransactionId());
        }
    }
}