package com.fastcampus.kafkahandson.model.message;

import com.fastcampus.kafkahandson.model.dto.OperationType;
import com.fastcampus.kafkahandson.model.dto.Payload;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MyCdcMessage {
    private int id;
    private Payload payload;
    private OperationType operationType;
    /**
     * C : before null -> after 0000
     * U : before 0000 -> after 0000
     * D : before 0000 -> after null
     */
}
