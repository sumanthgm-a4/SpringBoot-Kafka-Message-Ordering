package com.sum.kafka_message_ordering.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.sum.kafka_message_ordering.publisher.OrderProducer;

@RequestMapping("/order-events")
@RestController
public class OrderEventsController {

    private final OrderProducer producer;

    public OrderEventsController(OrderProducer producer) {
        this.producer = producer;
    }


    @PostMapping("/process")
    public void processOrder(@RequestParam String orderId) throws InterruptedException{
        producer.processOrder(orderId);
    }

    @PostMapping("/processWithKey")
    public void processOrderWithKey(@RequestParam String orderId){
        producer.processOrderWithPartitionKey(orderId);
    }


}