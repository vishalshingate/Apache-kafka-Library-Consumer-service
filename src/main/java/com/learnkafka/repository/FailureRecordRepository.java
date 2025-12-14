package com.learnkafka.repository;

import com.learnkafka.config.LibraryEventsConsumerConfigDbRecovery;
import com.learnkafka.model.FailureRecord;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface FailureRecordRepository extends CrudRepository<FailureRecord,Integer> {
    List<FailureRecord> findAllByStatus(String status);
}
