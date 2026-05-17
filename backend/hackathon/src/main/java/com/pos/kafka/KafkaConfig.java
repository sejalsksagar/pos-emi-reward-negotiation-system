package com.pos.kafka;


import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
public class KafkaConfig {
	
	@Bean
	public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {

	    DeadLetterPublishingRecoverer recoverer =
	            new DeadLetterPublishingRecoverer(kafkaTemplate,
	                    (record, ex) -> new TopicPartition("transactions-dlq", record.partition()));

	    return new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3));
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
	        ConsumerFactory<String, Object> consumerFactory,
	        DefaultErrorHandler errorHandler) {

	    ConcurrentKafkaListenerContainerFactory<String, Object> factory =
	            new ConcurrentKafkaListenerContainerFactory<>();

	    factory.setConsumerFactory(consumerFactory);
	    factory.setCommonErrorHandler(errorHandler);

	    return factory;
	}
}
