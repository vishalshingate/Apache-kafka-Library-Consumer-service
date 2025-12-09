package com.learnkafka.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.LibraryEventsConsumerApplication;
import com.learnkafka.consumer.LibraryEventsConsumer;
import com.learnkafka.model.Book;
import com.learnkafka.model.LibraryEvent;
import com.learnkafka.model.LibraryEventType;
import com.learnkafka.repository.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = LibraryEventsConsumerApplication.class
)
@EmbeddedKafka(topics = "library-events")
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        for (MessageListenerContainer container : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 123,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot\",\n" +
                "        \"bookAuthor\": \"Dilip\"\n" +
                "    },\n" +
                "    \"libraryEventType\": \"NEW\"\n" +
                "}";

        kafkaTemplate.sendDefault(json).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(123, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, InterruptedException, ExecutionException {
        // save a library event
        String json = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 123,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot\",\n" +
                "        \"bookAuthor\": \"Dilip\"\n" +
                "    },\n" +
                "    \"libraryEventType\": \"NEW\"\n" +
                "}";

        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEventsRepository.save(libraryEvent);

        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        // send the update event
        Book updatedBook = Book.builder()
                .bookId(123)
                .bookName("Kafka Using Spring Boot - Updated")
                .bookAuthor("Dilip - Updated")
                .build();
        libraryEvent.setBook(updatedBook);

        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent persitedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();

        assertEquals("Kafka Using Spring Boot - Updated", persitedLibraryEvent.getBook().getBookName());

    }

    @Test
    void publishUpdateLibraryEvent_null_LibraryEvent() throws JsonProcessingException, InterruptedException, ExecutionException {
        String json = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 123,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot\",\n" +
                "        \"bookAuthor\": \"Dilip\"\n" +
                "    },\n" +
                "    \"libraryEventType\": \"UPDATE\"\n" +
                "}";
        kafkaTemplate.sendDefault(json).get();
        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(6)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(6)).processLibraryEvent(isA(ConsumerRecord.class));


    }


}
