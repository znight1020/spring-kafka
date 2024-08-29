package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Slf4j
@Component
public class MyConsumer implements Consumer<Message<MyMessage>> {

    MyConsumer() {
      log.info("MyConsumer init!");
    }
    @Override
    public void accept(Message<MyMessage> message) {
        log.info("Message arrived! - {}", message.getPayload());
    }
}
