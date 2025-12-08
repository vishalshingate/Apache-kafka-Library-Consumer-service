package com.learnkafka.model;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {
    @Id
    private Integer bookId;
    private String bookName;
    private String bookAuthor;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "libraryEventId")
    @ToString.Exclude
    private LibraryEvent libraryEvent;
}
