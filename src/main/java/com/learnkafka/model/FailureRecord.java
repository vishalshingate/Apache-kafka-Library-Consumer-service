package com.learnkafka.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class FailureRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Getter
    private Integer id;
    @Getter
    @Setter
    private String topic;
    @Getter
    @Setter
    private Integer Key_value;
    @Getter
    @Setter
    private String errorRecord;
    @Getter
    @Setter
    private Integer partitionNo;
    @Getter
    @Setter
    private Long offset_value;
    @Getter
    @Setter
    private  String exception;
    @Getter
    @Setter
    private String status;


}
