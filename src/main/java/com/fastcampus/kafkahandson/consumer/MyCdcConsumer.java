package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.common.CustomObjectMapper;
import com.fastcampus.kafkahandson.model.message.MyCdcMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.fastcampus.kafkahandson.model.Topic.MY_CDC_TOPIC;

@Slf4j
@Component
@RequiredArgsConstructor
public class MyCdcConsumer {
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = { MY_CDC_TOPIC },
            groupId = "cdc-consumer-group",
            concurrency = "1"
    )

    public void listen(ConsumerRecord<String, String> message, Acknowledgment ack) throws JsonProcessingException {
        MyCdcMessage myCdcMessage = objectMapper.readValue(message.value(), MyCdcMessage.class);
        log.info("[CDC Consumer] Message arrived! - {}", myCdcMessage.getPayload());
        ack.acknowledge();
    }
}
