package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@Slf4j
@Component
public class MyBatchConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Integer> idHistoryMap = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @KafkaListener(
            topics = { MY_JSON_TOPIC },
            groupId = "batch-test-consumer-group",
            concurrency = "3", // 하나의 쓰레드가 아닌 3개의 쓰레드가 처리한다.
            containerFactory = "batchKafkaListenerContainerFactory"
    )

    public void accept(List<ConsumerRecord<String, String>> messages, Acknowledgment ack) {
        ObjectMapper objectMapper = new ObjectMapper();
        // log.info("[Batch Consumer] Message arrived! - count {}", messages.size());
        messages.forEach(message -> executorService.submit(() -> {
            MyMessage myMessage;
            try {
                myMessage = objectMapper.readValue(message.value(), MyMessage.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            printPayloadIfFirstMessage(message, myMessage);
        }));
        ack.acknowledge();
    }

    private void printPayloadIfFirstMessage(ConsumerRecord<String, String> message, MyMessage myMessage) {
        if (idHistoryMap.putIfAbsent(String.valueOf(myMessage.getId()), 1) == null) {
            log.info("ㄴ [Batch Consumer (Thread ID:{})] [Offset {} / Partition {}] / Value {}", Thread.currentThread().getId(), message.offset(), message.partition(), myMessage);
        } else {
            log.warn("[Batch Consumer] Duplicate! ({})", myMessage.getId());
        }
    }
}
