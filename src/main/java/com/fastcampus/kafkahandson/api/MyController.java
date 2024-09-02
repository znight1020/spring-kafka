package com.fastcampus.kafkahandson.api;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fastcampus.kafkahandson.model.MyModel;
import com.fastcampus.kafkahandson.model.Request;
import com.fastcampus.kafkahandson.producer.MyProducer;
import com.fastcampus.kafkahandson.service.MyService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RequiredArgsConstructor
@RestController
public class MyController {
    private final MyService myService;
    /**
     * CRUD HttpStatus를 위한 상세한 핸들링 생략
     */
    @PostMapping("/greeting")
    MyModel create(@RequestBody Request request) {
        if (request == null ||
                request.getUserId() == null ||
                request.getUserName() == null ||
                request.getUserAge() == null ||
                request.getContent() == null) {
            return null;
        }
        MyModel myModel = MyModel.create(
                request.getUserId(),
                request.getUserAge(),
                request.getUserName(),
                request.getContent()
        );
        return myService.save(myModel);
    }

    @GetMapping("/greetings/{id}")
    MyModel get(@PathVariable Integer id){
        return myService.findById(id);
    }

    @PatchMapping("/greetings/{id}")
    MyModel update(@PathVariable Integer id, @RequestBody String content) {
        if (id == null || content == null || content.isBlank()) return null;
        MyModel myModel = myService.findById(id);
        myModel.setContent(content);
        return myService.save(myModel);
    }

    @DeleteMapping("/greetings/{id}")
    void delete(@PathVariable Integer id){
        myService.delete(id);
    }
}
