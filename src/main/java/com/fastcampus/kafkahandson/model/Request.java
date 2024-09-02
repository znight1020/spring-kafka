package com.fastcampus.kafkahandson.model;

import lombok.Data;

@Data
public class Request {
    Integer userId;
    String userName;
    Integer userAge;
    String content;
}
