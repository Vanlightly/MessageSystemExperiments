package com.jackvanlightly.multitopicordering.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BufferedMessage {
    private ConsumerRecord<String,String> record;
    private KafkaConsumer<String, String> consumer;
    private String consumerId;

    public BufferedMessage(ConsumerRecord<String, String> record, KafkaConsumer<String, String> consumer, String consumerId) {
        this.record = record;
        this.consumer = consumer;
        this.consumerId = consumerId;
    }

    public ConsumerRecord<String, String> getRecord() {
        return record;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public String getConsumerId() {
        return consumerId;
    }
}
