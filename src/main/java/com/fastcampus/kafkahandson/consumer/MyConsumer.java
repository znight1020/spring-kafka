package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MyConsumer {

    MyConsumer() {
      log.info("MyConsumer init!");
    }

    @KafkaListener(
            topics = { "my-json-topic" },
            groupId = "test-consumer-group"
    )

    public void accept(ConsumerRecord<String, MyMessage> message) {
        log.info("Message arrived! - {}", message.value());
    }
}
