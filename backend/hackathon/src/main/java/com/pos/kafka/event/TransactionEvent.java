package com.pos.kafka.event;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TransactionEvent {

    private String transactionId;
    private String userId;
    private Double amount;
    private String status; // PENDING, PROCESSING
}
