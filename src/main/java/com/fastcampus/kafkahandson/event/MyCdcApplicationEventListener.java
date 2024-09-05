package com.fastcampus.kafkahandson.event;

import com.fastcampus.kafkahandson.model.dto.MyModelConverter;
import com.fastcampus.kafkahandson.producer.MyCdcProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@RequiredArgsConstructor
@Component
public class MyCdcApplicationEventListener {

    private final MyCdcProducer producer;

    /* 미사용 */
    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT) //  트랜잭션의 커밋 이후에 이벤트를 처리하도록 지정
    public void transactionalEventListenerAfterCommit(MyCdcApplicationEvent event) throws JsonProcessingException {
        producer.sendMessage(
                MyModelConverter.toMessage(event.getId(), event.getMyModel(), event.getOperationType())
        );
    }
}
