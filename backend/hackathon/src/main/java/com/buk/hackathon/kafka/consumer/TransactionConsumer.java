package com.buk.hackathon.kafka.consumer;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.buk.hackathon.kafka.event.TransactionEvent;
import com.buk.hackathon.service.TransactionProcessingService;

@RequiredArgsConstructor
@Service
@Slf4j
public class TransactionConsumer {

    private final TransactionProcessingService processingService;

    @KafkaListener(topics = "${kafka.topic.transactions}", groupId = "pos-group")
    public void consume(TransactionEvent event) {

        log.info("Received transaction event: {}", event);

        processingService.process(
                event.getTransactionId(),
                event.getUserId(),
                event.getAmount()
        );
    }
}
