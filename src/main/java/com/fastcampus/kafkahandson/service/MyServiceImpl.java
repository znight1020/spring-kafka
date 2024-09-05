package com.fastcampus.kafkahandson.service;

import com.fastcampus.kafkahandson.data.MyEntity;
import com.fastcampus.kafkahandson.data.MyJpaRepository;
import com.fastcampus.kafkahandson.event.MyCdcApplicationEvent;
import com.fastcampus.kafkahandson.model.dto.MyModel;
import com.fastcampus.kafkahandson.model.dto.MyModelConverter;
import com.fastcampus.kafkahandson.model.dto.OperationType;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
@Service
public class MyServiceImpl implements MyService{

    private final MyJpaRepository myJpaRepository;

    @Override
    public List<MyModel> findAll() {
        List<MyEntity> entities = myJpaRepository.findAll();
        return entities.stream().map(MyModelConverter::toModel).toList();
    }

    @Override
    public MyModel findById(Integer id) {
        Optional<MyEntity> entity = myJpaRepository.findById(id);
        return entity.map(MyModelConverter::toModel).orElse(null);
    }

    @Override
    public MyModel save(MyModel model) {
        MyEntity entity = myJpaRepository.save(MyModelConverter.toEntity(model));
        MyModel resultModel = MyModelConverter.toModel(entity);

        return resultModel;
    }

    @Override
    public void delete(Integer id) {
        myJpaRepository.deleteById(id);
    }

    /**
     * @TransactionalEventListener 를 이용한 CDC
     * 이벤트를 처리하는 도중 예외가 발생하였다면, kafka produce 작업이 이루어지지 않지만, DB에는 반영되어있는 상태가 되는 문제가 발생할 수 있다.
     * */
    /*@Override
    @Transactional
    public MyModel save(MyModel model) { // CREATE : id null / UPDATE id not null
        OperationType operationType = model.getId() == null ? OperationType.CREATE : OperationType.UPDATE;
        MyEntity entity = myJpaRepository.save(MyModelConverter.toEntity(model));

        MyModel resultModel = MyModelConverter.toModel(entity);
        applicationEventPublisher.publishEvent(
                new MyCdcApplicationEvent(
                        this,
                        entity.getId(),
                        resultModel,
                        operationType
                )
        );
        return resultModel;
    }

    @Override
    @Transactional
    public void delete(Integer id) {
        myJpaRepository.deleteById(id);
        applicationEventPublisher.publishEvent(
                new MyCdcApplicationEvent(
                        this,
                        id,
                        null,
                        OperationType.DELETE
                )
        );
    }*/


    /**
     * @Transactional 을 이용한 CDC
     * 해당 방법에서는 kafka message는 produce 되어있고, DB는 예외가 발생했으므로 테이블에 데이터가 들어가지 않는 문제가 발생할 수 있다.
     * */
    /*@Override
    @Transactional
    public MyModel save(MyModel model) { // CREATE : id null / UPDATE id not null
        OperationType operationType = model.getId() == null ? OperationType.CREATE : OperationType.UPDATE;
        MyEntity entity = myJpaRepository.save(MyModelConverter.toEntity(model));
        try {
            myCdcProducer.sendMessage(
                    MyModelConverter.toMessage(
                            entity.getId(),
                            MyModelConverter.toModel(entity),
                            operationType
                    )
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error processing JSON for sendMessage", e);
        }
        return MyModelConverter.toModel(entity);
    }*/

    /*@Override
    @Transactional
    public void delete(Integer id) {
        myJpaRepository.deleteById(id);
        try {
            myCdcProducer.sendMessage(
                    MyModelConverter.toMessage(
                            id,
                            null,
                            OperationType.DELETE
                    )
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error processing JSON for sendMessage", e);
        }
    }*/
}
