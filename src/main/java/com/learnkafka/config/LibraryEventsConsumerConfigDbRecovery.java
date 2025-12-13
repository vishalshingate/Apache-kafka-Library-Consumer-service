package com.learnkafka.config;

import com.learnkafka.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;
@Configuration
@Slf4j
/*
  This class has same implementation as LibraryEventsConsumerConfig but here we are simulating DB recovery
  So when RecoverableDataAccessException is thrown then we will save record to db.
 */
public class LibraryEventsConsumerConfigDbRecovery {
    private final KafkaProperties properties;
    private final KafkaTemplate<?, ?> kafkaTemplate;
    @Autowired
    private FailureService failureService;

    private static final String RETRY = "RETRY";
    private static final String DEAD = "DEAD";

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlq}")
    private String dlqTopic;



    public LibraryEventsConsumerConfigDbRecovery(KafkaProperties properties, KafkaTemplate<?, ?> kafkaTemplate) {
        this.properties = properties;
        this.kafkaTemplate = kafkaTemplate;

    }




    private ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, exception) -> {
       {
            if(exception.getCause() instanceof RecoverableDataAccessException ) {
                log.info("Recoverable data access exception");
              // recoverable logic invoke a method to save the record to DB
                failureService.saveFailedRecord(consumerRecord, exception, RETRY);
            } else {
               // non-recoverable logic
                failureService.saveFailedRecord(consumerRecord, exception, DEAD);
            }
        }};



    public DefaultErrorHandler errorHandler() {
        //custom error handler logic
        var fixedBackOff= new FixedBackOff(1000L, 2L); // retry every 1 second, max 2 retries
        /**
         * this is custom error handler which will handle the exceptions thrown by the listener
         * we can configure the retry logic here
         * we can configure the dead letter topic here or Recovery logic here
         */
        var errorHandler = new DefaultErrorHandler(
          //commented following code to simulate DB recovery instead of publishing to retry topic
           // publishingRecoverer(), // error handler which will publish the failed records to dlq or retry topic
            consumerRecordRecoverer,
            fixedBackOff
        );

        var exceptionsToIgnoreList= List.of(
            IllegalArgumentException.class
        );

        errorHandler
            .setRetryListeners(((record, ex, deliveryAttempt) -> {
                log.info("Failed Record in retry Listener , Exception {}, deliveryAttempt {} ", ex.getMessage(), deliveryAttempt);
            }));

        errorHandler.addNotRetryableExceptions();
        // similar to the addNotRetryableExceptions we have addRetryableExceptions method also where we can put those exceptions which we want to retry
        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
        return errorHandler;
    }



    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
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


}
