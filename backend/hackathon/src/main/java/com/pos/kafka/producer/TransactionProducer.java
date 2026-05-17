package com.pos.kafka.producer;



import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.pos.kafka.event.TransactionEvent;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionProducer {

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    @Value("${kafka.topic.transactions}")
    private String topic;

    public void sendTransactionEvent(TransactionEvent event) {
        log.info("Sending transaction event: {}", event);

        kafkaTemplate.send(topic, event.getTransactionId(), event);
    }
}
