package com.sum.kafka_message_ordering.publisher;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.sum.kafka_message_ordering.events.OrderEvent;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class OrderProducer {

    @Value("${order.processing.topic-name}")
    private String topicName;

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;


    // send WITHOUT key -> Kafka will use default partitioning (sticky/round-robin for null keys)
    public void sendWithoutKey(OrderEvent event) {
        kafkaTemplate.send(topicName, event);
        System.out.println("Event produced : " + event);
    }

    // send WITH key -> ensures same key hashes to same partition (ordering per key)
    public void sendWithKey(OrderEvent event) {
        kafkaTemplate.send(topicName, event.orderId(), event);
        System.out.println("Event produced : " + event);
    }


    public void processOrder(String orderId) {
        System.out.println("Processing order without key: " + orderId);
        kafkaTemplate.send(topicName,
                new OrderEvent(orderId, 1, "OrderPlaced", Instant.now()));
        kafkaTemplate.send(topicName,
                new OrderEvent(orderId, 2, "PaymentProcessed", Instant.now()));
        kafkaTemplate.send(topicName,
                new OrderEvent(orderId, 3, "OrderShipped", Instant.now()));
        kafkaTemplate.send(topicName,
                new OrderEvent(orderId, 4, "OrderDelivered", Instant.now()));
        System.out.println("Completed processing order without key: " + orderId);
    }

    public void processOrderWithPartitionKey(String orderId) {
        System.out.println("Processing order with key: " + orderId);
        kafkaTemplate.send(topicName,orderId,
                new OrderEvent(orderId, 1, "OrderPlaced", Instant.now()));
        kafkaTemplate.send(topicName,orderId,
                new OrderEvent(orderId, 2, "PaymentProcessed", Instant.now()));
        kafkaTemplate.send(topicName,orderId,
                new OrderEvent(orderId, 3, "OrderShipped", Instant.now()));
        kafkaTemplate.send(topicName,orderId,
                new OrderEvent(orderId, 4, "OrderDelivered", Instant.now()));
        System.out.println("Completed processing order with key: " + orderId);
    }













//    // send WITHOUT key -> Kafka will use default partitioning (sticky/round-robin for null keys)
//    public void sendWithoutKey(OrderEvent event) {
//        kafkaTemplate.send(topicName, event);
//        System.out.println("Event produced : " + event);
//    }
//
//    // send WITH key -> ensures same key hashes to same partition (ordering per key)
//    public void sendWithKey(OrderEvent event) {
//        kafkaTemplate.send(topicName, event.orderId(), event);
//        System.out.println("Event produced : " + event);
//    }





}