package com.pos.kafka.consumer;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.pos.kafka.event.TransactionEvent;
import com.pos.service.TransactionProcessingService;

@RequiredArgsConstructor
@Service
@Slf4j
public class TransactionConsumer {

    private final TransactionProcessingService processingService;

    @KafkaListener(topics = "${kafka.topic.transactions}", groupId = "pos-group")
    public void consume(TransactionEvent event) {
    	
    	try {
            log.info("Received transaction event: {}", event);
            //throw new RuntimeException("Test failure"); //test
            
            processingService.process(event);
    	} catch (Exception ex) {
    		event.setStatus("FAILED");
            log.error("Processing failed for txn {}", event.getTransactionId(), ex);
            
            //processingService.markFailed(event.getTransactionId());

            throw ex; // important for DLQ
        }

    }
}
