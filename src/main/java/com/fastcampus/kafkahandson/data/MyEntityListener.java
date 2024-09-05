package com.fastcampus.kafkahandson.data;

import com.fastcampus.kafkahandson.model.dto.MyModel;
import com.fastcampus.kafkahandson.model.dto.MyModelConverter;
import com.fastcampus.kafkahandson.model.dto.OperationType;
import com.fastcampus.kafkahandson.producer.MyCdcProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PostRemove;
import jakarta.persistence.PostUpdate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class MyEntityListener {

    @Lazy
    private final MyCdcProducer producer;

    // DB의 row가 create 되면 @PostPersist 를 통해 listen할 수 있다.
    @PostPersist
    public void handleCreate(MyEntity myEntity) {
        log.info("handle create");

        MyModel myModel = MyModelConverter.toModel(myEntity);
        try {
            producer.sendMessage(
                    MyModelConverter.toMessage(myEntity.getId(), myModel, OperationType.CREATE)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @PostUpdate
    public void handleUpdate(MyEntity myEntity) {
        log.info("handle update");

        MyModel myModel = MyModelConverter.toModel(myEntity);
        try {
            producer.sendMessage(
                    MyModelConverter.toMessage(myEntity.getId(), myModel, OperationType.UPDATE)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @PostRemove
    public void handleDelete(MyEntity myEntity) {
        log.info("handle delete");

        MyModel myModel = MyModelConverter.toModel(myEntity);
        try {
            producer.sendMessage(
                    MyModelConverter.toMessage(myEntity.getId(), null, OperationType.DELETE)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
