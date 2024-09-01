package com.fastcampus.kafkahandson.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.fastcampus.kafkahandson.model.Topic.MY_SECOND_TOPIC;

@Slf4j
@Component
public class MySecondConsumer {

    MySecondConsumer() {
      log.info("MyConsumer init!");
    }

    @KafkaListener(
            topics = { MY_SECOND_TOPIC },
            groupId = "test-consumer-group",
            containerFactory = "secondKafkaListenerContainerFactory"
    )

    public void accept(ConsumerRecord<String, String> message) {
        log.info("[Second Consumer] Message arrived! - {}", message.value());
        log.info("[Second Consumer] Offset : {}, Partition : {}, ", message.offset(), message.partition() );
    }
}
