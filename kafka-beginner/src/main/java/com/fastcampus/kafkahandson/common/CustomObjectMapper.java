package com.fastcampus.kafkahandson.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.stereotype.Component;

@Component
public class CustomObjectMapper extends ObjectMapper {
    public CustomObjectMapper() {
        registerModule(new JavaTimeModule());
    }
}
