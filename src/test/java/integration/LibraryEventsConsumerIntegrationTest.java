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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = LibraryEventsConsumerApplication.class
)
@EmbeddedKafka(topics = {"library-events", "library-events-retry", "library-events-dlq"}, partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "retry.listener.startup=false" })

//retry.listener.startup=false" this should be same name as defined property name in the listener class
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

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlq}")
    private String dlqTopic;

    private Consumer<Integer,String> consumer;


    @BeforeEach
    public void setup() {
//        for (MessageListenerContainer container : endpointRegistry.getListenerContainers()) {
//            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
//        }

        var container = endpointRegistry.getAllListenerContainers()
            .stream().filter(messageListenerContainer ->
                Objects.equals(messageListenerContainer.getGroupId(), "library-events-group"))
            .toList().get(0);
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
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
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        var configs = new HashMap<>(
            KafkaTestUtils.consumerProps("group2", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
            .createConsumer();


        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, dlqTopic);
        ConsumerRecord<Integer, String> consumerRecords = KafkaTestUtils.getSingleRecord(consumer, dlqTopic);
        System.out.println("ConsumerRecord is: "+consumerRecords);
        assertEquals(json, consumerRecords.value());

    }
    @Test
    void publishUpdateLibraryEvent_999_LibraryEvent() throws JsonProcessingException, InterruptedException, ExecutionException {
        String json = "{\n" +
            "    \"libraryEventId\": 999,\n" +
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
        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        var configs = new HashMap<>(
            KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
            .createConsumer();

        // subscribe consumer to embedded kafka topics
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecords = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
        System.out.println("ConsumerRecord is: "+consumerRecords);
        assertEquals(json, consumerRecords.value());

    }


}
