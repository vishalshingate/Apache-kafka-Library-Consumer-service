package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsRetryConsumer {
  private final LibraryEventsService libraryEventsService;


  public  LibraryEventsRetryConsumer(LibraryEventsService libraryEventsService) {
      this.libraryEventsService = libraryEventsService;
  }
    @KafkaListener(
        topics = {"${topics.retry}"},
        groupId = "rerty-listener-group",
        autoStartup = "${retry.listener.startup:true}" // if this property is not provided default will be true , (false) this listener will not start automatically
    ) // this topic name should match with the producer topic name
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Message received received in retry Listener: {}", consumerRecord);
        consumerRecord.headers()
                .forEach(header -> {
                    log.info("key:{}, value:{}", header.key(), new String(header.value()));
                });
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
