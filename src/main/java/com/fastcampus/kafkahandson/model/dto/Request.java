package com.fastcampus.kafkahandson.model.dto;

import lombok.Data;

@Data
public class Request {
    Integer userId;
    String userName;
    Integer userAge;
    String content;
}
