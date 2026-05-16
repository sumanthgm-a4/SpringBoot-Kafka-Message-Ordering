package com.sum.kafka_message_ordering.events;


import java.time.Instant;

public record OrderEvent (String orderId, int seq, String eventType, Instant ts){}