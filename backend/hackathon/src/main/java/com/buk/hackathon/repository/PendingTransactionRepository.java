package com.buk.hackathon.repository;


import org.springframework.data.mongodb.repository.MongoRepository;

import com.buk.hackathon.entity.PendingTransaction;

public interface PendingTransactionRepository extends MongoRepository<PendingTransaction, String> {
}
