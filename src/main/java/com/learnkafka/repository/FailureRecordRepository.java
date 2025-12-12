package com.learnkafka.repository;

import com.learnkafka.model.FailureRecord;
import org.springframework.data.repository.CrudRepository;

public interface FailureRecordRepository extends CrudRepository<FailureRecord,Integer> {
}
