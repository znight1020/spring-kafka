package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyCdcMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public class MyCdcConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    // ConcurrentHashMap 을 사용해야 한줄 서기가 가능하다.
    // 보통은 Redis 와 같이 key 저장소를 두지만 개념적으로 하기 위해 메모리 방식으로 구현
    // id 에 대해 Exactly Once 를 보장하기 위함
    private final Map<String, Integer> idHistoryMap = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = { MY_CDC_TOPIC },
            groupId = "cdc-consumer-group",
            concurrency = "1"
    )

    public void accept(ConsumerRecord<String, String> message, Acknowledgment ack) throws JsonProcessingException {
        MyCdcMessage myCdcMessage = objectMapper.readValue(message.value(), MyCdcMessage.class);
        log.info("[CDC Consumer] Message arrived! - {}", myCdcMessage.getPayload());
        ack.acknowledge(); // 수동 커밋
    }
}
