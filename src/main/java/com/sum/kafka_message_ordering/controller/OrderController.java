package com.sum.kafka_message_ordering.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.sum.kafka_message_ordering.events.OrderEvent;
import com.sum.kafka_message_ordering.publisher.OrderProducer;

import java.time.Instant;

@RestController
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    //Run this demo endpoints to see difference in ordering with and without keys
    //different partition
    @PostMapping("/demo/no-key")
    public String demoNoKey(@RequestParam String orderId, @RequestParam(defaultValue = "10") int count) {
        for (int i = 1; i <= count; i++) {
            producer.sendWithoutKey(new OrderEvent(orderId, i, "EV_" + i, Instant.now()));
            // small pause optional: Thread.sleep(5);
        }
        return "sent no-key";
    }

    // Run this demo endpoints to see difference in ordering with keys
    // single partition
    @PostMapping("/demo/with-key")
    public String demoWithKey(@RequestParam String orderId, @RequestParam(defaultValue = "10") int count) {
        for (int i = 1; i <= count; i++) {
            producer.sendWithKey(new OrderEvent(orderId, i, "EV_" + i, Instant.now()));
        }
        return "sent with-key";
    }

}