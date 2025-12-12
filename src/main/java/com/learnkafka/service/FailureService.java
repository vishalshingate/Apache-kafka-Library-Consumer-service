package com.learnkafka.service;

import com.learnkafka.model.FailureRecord;
import com.learnkafka.repository.FailureRecordRepository;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureService {

    private final FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<?,?> consumerRecord, Exception exception, String status) {
        FailureRecord failureRecord = new FailureRecord();
        failureRecord.setTopic(consumerRecord.topic());
        failureRecord.setPartitionNo(consumerRecord.partition());
        failureRecord.setOffset_value(consumerRecord.offset());
        failureRecord.setKey_value(consumerRecord.key() != null ? (Integer) consumerRecord.key() : null);
        failureRecord.setErrorRecord(consumerRecord.value() != null ? consumerRecord.value().toString() : null);
        failureRecord.setException(exception.getMessage());
        failureRecord.setStatus(status);
        failureRecordRepository.save(failureRecord);
        log.info("Saved failed record to DB: {}", failureRecord);
    }
}
