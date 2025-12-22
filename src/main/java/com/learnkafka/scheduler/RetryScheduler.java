package com.learnkafka.scheduler;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.config.LibraryEventsConsumerConfigDbRecovery;
import com.learnkafka.model.FailureRecord;
import com.learnkafka.repository.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Slf4j
public class RetryScheduler {

    private final Long schedularRate = 10000L;

    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventsService libraryEventsService;
    public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventsService libraryEventsService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventsService = libraryEventsService;
    }


    @Scheduled( fixedRate = 10000 )
    @Transactional
    public void retryFailedRecords() {
        log.info("Retry schedular started");
        failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfigDbRecovery.RETRY)
            .forEach(failureRecord -> {
                log.info("Retrying failed record: {}", failureRecord);
                var consumerRecord = buildConsumerRecord(failureRecord);
                // calling the processLibraryEvent method to reprocess the failed record
                try {
                    libraryEventsService.processLibraryEvent(consumerRecord);
                    log.info("Successfully processed record: {}", consumerRecord);
                    failureRecord.setStatus(LibraryEventsConsumerConfigDbRecovery.SUCCESS);
                    // Explicitly persist the updated status
                    failureRecordRepository.save(failureRecord);
                } catch (JsonProcessingException e) {
                    log.error("Failed to process library event {}", e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            });
        log.info("Retry schedular completed");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {

        return new ConsumerRecord<>(
            failureRecord.getTopic(),
            failureRecord.getPartitionNo(),
            failureRecord.getOffset_value(),
            failureRecord.getKey_value(),
            failureRecord.getErrorRecord()
        );

    }
}
