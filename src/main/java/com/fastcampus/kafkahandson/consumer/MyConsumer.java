package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.common.CustomObjectMapper;
import com.fastcampus.kafkahandson.model.message.MyMessage;
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

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@Slf4j
@Component
@RequiredArgsConstructor
public class MyConsumer {
    private final CustomObjectMapper objectMapper;

    // ConcurrentHashMap 을 사용해야 한줄 서기가 가능하다.
    // 보통은 Redis 와 같이 key 저장소를 두지만 개념적으로 하기 위해 메모리 방식으로 구현
    // id 에 대해 Exactly Once 를 보장하기 위함
    private final Map<String, Integer> idHistoryMap = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = { MY_JSON_TOPIC },
            groupId = "test-consumer-group",
            concurrency = "3"
    )

    public void listen(ConsumerRecord<String, String> message, Acknowledgment ack) throws JsonProcessingException {
        MyMessage myMessage = objectMapper.readValue(message.value(), MyMessage.class);
        printPayloadIfFirstMessage(myMessage);
        ack.acknowledge(); // 수동 커밋
    }

    // synchronized 를 넣어준 이유는 Map.get(), Map.put() 사이에 다른 쓰레드에서 접근할 수 있기 때문에
    // 한줄서기를 할 수 있게 지정해 준 것이다.
    private synchronized void printPayloadIfFirstMessage(MyMessage myMessage) {
        // id 에 대해서 unique 하게 지정
        if (idHistoryMap.get(String.valueOf(myMessage.getId())) == null) {
            // Exactly Once 실행되어야 하는 로직이라 가정
            log.info("[Main Consumer] Message arrived! - {}", myMessage);
            idHistoryMap.put(String.valueOf(myMessage.getId()), 1);
        } else {
            log.warn("[Main Consumer] Duplicate! ({})", myMessage.getId());
        }
    }

    /*private void printPayloadIfFirstMessage(MyMessage myMessage) {
        if (idHistoryMap.putIfAbsent(String.valueOf(myMessage.getId()), 1) == null) {
            log.info("[Main Consumer] Message arrived! - {}", myMessage);
        } else {
            log.warn("[Main Consumer] Duplicate! ({})", myMessage.getId());
        }
    }*/
}
