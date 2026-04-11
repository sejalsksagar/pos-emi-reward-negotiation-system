package com.buk.hackathon.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.buk.hackathon.entity.TransactionEntity;

public interface TransactionRepository extends JpaRepository<TransactionEntity, String> {
}
