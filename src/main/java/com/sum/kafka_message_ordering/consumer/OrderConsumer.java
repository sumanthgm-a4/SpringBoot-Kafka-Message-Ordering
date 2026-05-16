package com.sum.kafka_message_ordering.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.sum.kafka_message_ordering.events.OrderEvent;

@Service
public class OrderConsumer {

    // @KafkaListener(topics = "${order.processing.topic-name}", groupId = "order-group")
    public void consume(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        System.out.printf("CONSUMER: partition=%d offset=%d orderId=%s seq=%d time=%s event=%s%n",
                record.partition(), record.offset(),
                event.orderId(), event.seq(), event.ts(), event.eventType());
    }
}