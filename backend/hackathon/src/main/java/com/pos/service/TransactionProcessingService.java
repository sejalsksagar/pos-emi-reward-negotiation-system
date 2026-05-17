package com.pos.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import org.springframework.stereotype.Service;

import com.pos.entity.Offer;
import com.pos.entity.PendingTransaction;
import com.pos.kafka.event.TransactionEvent;
import com.pos.repository.PendingTransactionRepository;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionProcessingService {

    private final PendingTransactionRepository pendingRepo;
    private final OfferService offerService;

    public void process(TransactionEvent event) {

        PendingTransaction txn = pendingRepo.findById(event.getTransactionId())
                .orElse(null);

        if (txn == null) return;

        if (!"PENDING".equals(txn.getStatus())) return;

        try {
            List<Offer> offers = offerService.generateOffers(
                    event.getUserId(),
                    event.getAmount()
            );

            txn.setOffers(offers);
            txn.setStatus("PROCESSING");

            pendingRepo.save(txn);

        } catch (Exception e) {

            log.error("Error processing txn {}", txn.getTransactionId(), e);

            // REMOVE from Mongo (key fix)
            pendingRepo.deleteById(txn.getTransactionId());

            throw e; // sends to DLQ
        }
    }
    
}
