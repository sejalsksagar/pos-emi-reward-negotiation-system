package com.pos.entity;


import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "pending_transactions")
public class PendingTransaction {

    @Id
    private String transactionId;

    private String userId;

    private Double amount;

    private String status;

    private Instant createdAt;
    
    private List<Offer> offers;
}
