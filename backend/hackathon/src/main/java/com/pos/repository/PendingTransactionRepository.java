package com.pos.repository;


import java.time.Instant;
import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.pos.entity.PendingTransaction;

public interface PendingTransactionRepository extends MongoRepository<PendingTransaction, String> {
	List<PendingTransaction> findByStatusAndCreatedAtBefore(
	        String status, Instant cutoff);
}
