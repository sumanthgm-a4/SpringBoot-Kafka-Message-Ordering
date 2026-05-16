package com.sum.kafka_message_ordering.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.sum.kafka_message_ordering.events.OrderEvent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class OrderEventConsumerParallel {

    private final ExecutorService executor = Executors
            .newFixedThreadPool(4);


    @KafkaListener(topics = "order-events",
            groupId = "order-group"
//            properties = {
//                    "max.poll.records=500"
//            }
    )
    public void consumeBatch(List<ConsumerRecord<String, OrderEvent>> records) {
        Map<Integer, ExecutorService> partitionExecutors = new ConcurrentHashMap<>();

        for (ConsumerRecord<String, OrderEvent> record : records) {

            executor
                    // .computeIfAbsent(record.partition(), p -> Executors.newSingleThreadExecutor())
                    .submit(() -> {
                        OrderEvent event = record.value();

                        System.out.printf("CONSUMER (parallel) -> partition=%d offset=%d orderId=%s seq=%d event=%s%n",
                                record.partition(),
                                record.offset(),
                                event.orderId(),
                                event.seq(),
                                event.eventType());
                    });
        }
    }


}