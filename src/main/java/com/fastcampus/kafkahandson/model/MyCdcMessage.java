package com.fastcampus.kafkahandson.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class MyCdcMessage {
    private int id;
    private Payload payload;
    private OperationType operationType;

    @Data
    @AllArgsConstructor
    public static class Payload {
        private int id;
        private int userId;
        private int userAge;
        private String userName;
        private String content;
        private final LocalDateTime createdAt;
        private final LocalDateTime updatedAt;
    }
    /**
     * C : before null -> after 0000
     * U : before 0000 -> after 0000
     * D : before 0000 -> after null
     */
}
