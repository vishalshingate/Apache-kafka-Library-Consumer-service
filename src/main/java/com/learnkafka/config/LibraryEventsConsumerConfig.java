package com.learnkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@Slf4j
//@EnableKafka for older version of kafka
public class LibraryEventsConsumerConfig {

    private final KafkaProperties properties;

    public LibraryEventsConsumerConfig(KafkaProperties properties) {
        this.properties = properties;
    }
    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
        ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
        ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
        ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
            .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler()); // we can use custom error handler here
        // this will create the 3 threads for 3 partitions of 3 listeners, so we will have 3 poll loops parallel pooling the records
        // this is not necessary in cloud env since we can scale the pods
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }

    public DefaultErrorHandler errorHandler() {
        //custom error handler logic
        var fixedBackOff= new FixedBackOff(1000L, 2L); // retry every 1 second, max 2 retries

        var errorHandler = new DefaultErrorHandler(fixedBackOff);
        var exceptionsToIgnoreList= List.of(
            IllegalArgumentException.class
        );

        errorHandler
            .setRetryListeners(((record, ex, deliveryAttempt) -> {
                log.info("Failed Record in retry Listener , Exception {}, deliveryAttempt {} ", ex.getMessage(), deliveryAttempt);
            }));

        errorHandler.addNotRetryableExceptions();

        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
        return errorHandler;
    }
}
