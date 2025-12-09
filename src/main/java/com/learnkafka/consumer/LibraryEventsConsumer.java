package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumer {

    private final LibraryEventsService libraryEventsService;

    public LibraryEventsConsumer(LibraryEventsService libraryEventsService) {
        this.libraryEventsService = libraryEventsService;
    }
    /**
     * Listens to messages from the "library-events" this is simple consumer method which has the poll call
     * it will poll multiple records but it will pass one by one to this method
     * @param consumerRecord  this will be the type of ConsumerRecords<?, ?>
     */
    @KafkaListener(topics = {"library-events"},
        groupId = "library-events-group"
    ) // this topic name should match with the producer topic name

    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Message received: {}", consumerRecord);
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
