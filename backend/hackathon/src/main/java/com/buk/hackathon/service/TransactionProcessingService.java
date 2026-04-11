package com.buk.hackathon.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import com.buk.hackathon.entity.PendingTransaction;
import com.buk.hackathon.repository.PendingTransactionRepository;

@Service
@RequiredArgsConstructor
public class TransactionProcessingService {

    private final PendingTransactionRepository pendingRepo;
    private final OfferService offerService;

    public void process(String transactionId, String userId, Double amount) {
    	System.out.println("Processing txn: " + transactionId);

        PendingTransaction txn = pendingRepo.findById(transactionId).orElseThrow();

        // Generate offers
        var offers = offerService.generateOffers(userId, amount);
        
        System.out.println("Generated offers: " + offers);

        txn.setOffers(offers);

        pendingRepo.save(txn);
    }
}
