package com.pos.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.pos.dto.CreateTransactionRequest;
import com.pos.dto.SelectOfferRequest;
import com.pos.entity.TransactionEntity;
import com.pos.repository.TransactionRepository;
import com.pos.service.TransactionService;

import lombok.RequiredArgsConstructor;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/transactions")
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionService transactionService;
    
    private final TransactionRepository transactionRepository;

    @PostMapping
    public ResponseEntity<String> createTransaction(@RequestBody CreateTransactionRequest request) {

        String transactionId = transactionService.createTransaction(request);

        return ResponseEntity.ok(transactionId);
    }
    
    @GetMapping("/{transactionId}/offers")
    public ResponseEntity<?> getOffers(@PathVariable String transactionId) {

        return ResponseEntity.ok(transactionService.getOffers(transactionId));
    }
    
    @PostMapping("/{transactionId}/select-offer")
    public ResponseEntity<?> selectOffer(
            @PathVariable String transactionId,
            @RequestBody SelectOfferRequest request) {

        transactionService.selectOffer(transactionId, request.getOfferId());

        return ResponseEntity.ok("Offer selected");
    }
    
    @GetMapping("/{transactionId}")
    public ResponseEntity<?> getFinalTransaction(@PathVariable String transactionId) {

		return ResponseEntity.ok(
                transactionRepository.findById(transactionId)
                        .orElseThrow(() -> new RuntimeException("Not found"))
        );
    }
    
    @GetMapping("/all")
    public List<TransactionEntity> getAll() {
        return transactionRepository.findAll();
    }
}