package com.fastcampus.kafkahandson.producer;

import com.fastcampus.kafkahandson.common.CustomObjectMapper;
import com.fastcampus.kafkahandson.model.message.MyMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@RequiredArgsConstructor
@Component
public class MyProducer {
    private final CustomObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(MyMessage myMessage) throws JsonProcessingException {
        kafkaTemplate.send(
                MY_JSON_TOPIC,
                String.valueOf(myMessage.getAge()),
                objectMapper.writeValueAsString(myMessage)
        );
    }
}
