package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.model.LibraryEvent;
import com.learnkafka.repository.LibraryEventsRepository;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final LibraryEventsRepository libraryEventsRepository;
    public LibraryEventsService(ObjectMapper objectMapper, LibraryEventsRepository libraryEventsRepository) {
        this.objectMapper = objectMapper;
        this.libraryEventsRepository = libraryEventsRepository;
    }

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;

            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.error("Invalid Library Event type");
        }
        log.info("Library Event received: {}", libraryEvent);
    }

    private void save(LibraryEvent libraryEvent) throws JsonProcessingException {

        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully saved Library Event: {}", libraryEvent);
    }

    private void validate(LibraryEvent libraryEvent) throws JsonProcessingException {
        if(libraryEvent.getLibraryEventId() == null){
            throw new IllegalArgumentException("Library Event Id is missing");
        }

        if(libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 999){
            throw new RecoverableDataAccessException("Library Event Id out of range LibraryEventId: 999");
        }
       Optional<LibraryEvent>libraryEventOptional= libraryEventsRepository.findById(libraryEvent.getLibraryEventId());

        if(!libraryEventOptional.isPresent()){
            throw new IllegalArgumentException("Library Event Id is not present in the database");
        }
        log.info("Validation is successful for the library event: {}", libraryEventOptional.get());

    }
}
