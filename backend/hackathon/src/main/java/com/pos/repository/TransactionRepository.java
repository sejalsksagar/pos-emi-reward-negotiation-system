package com.pos.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.pos.entity.TransactionEntity;

public interface TransactionRepository extends JpaRepository<TransactionEntity, String> {
}
