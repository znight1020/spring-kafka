package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@Slf4j
@Component
public class MyThirdConsumer {
    @KafkaListener(
            topics = { MY_JSON_TOPIC },
            groupId = "batch-test-consumer-group",
            containerFactory = "batchKafkaListenerContainerFactory"
    )

    public void accept(List<ConsumerRecord<String, String>> messages) {
        ObjectMapper objectMapper = new ObjectMapper();
        log.info("[Third Consumer] Message arrived! - count {}", messages.size());
        messages.forEach(message -> {
            MyMessage myMessage;
            try {
                myMessage = objectMapper.readValue(message.value(), MyMessage.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            log.info("ㄴ [Third Consumer] Value {} / Offset {} / Partition {} ", myMessage, message.offset(), message.partition());
        });
    }
}
