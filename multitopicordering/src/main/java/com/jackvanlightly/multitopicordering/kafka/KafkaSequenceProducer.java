package com.jackvanlightly.multitopicordering.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaSequenceProducer {
    private boolean isCancelled;
    private boolean isCompleted;

    private List<String> topics;
    private Properties props;
    List<Integer> msgCounts;
    Random rand = new Random();

    public KafkaSequenceProducer(List<String> topics, List<Integer> msgCounts) {
        this.topics = topics;
        if(msgCounts == null)
            this.msgCounts = topics.stream().map(x -> 1).collect(Collectors.toList());
        else
            this.msgCounts = msgCounts;
        initProps();
    }

    public KafkaSequenceProducer(List<String> topics, List<Integer> msgCounts, Properties customProps) {
        this(topics,msgCounts);

        for(Map.Entry prop : customProps.entrySet())
            props.put(prop.getKey(), prop.getValue());
    }

    private void initProps() {
        props = new Properties();
        props.put("bootstrap.servers", KafkaConnection.BootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
    }

    public void declareTopics() {
        Producer<String, String> producer = new KafkaProducer<>(props);

        for(String topic : topics) {
            String msg = "start";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "NoKey", msg);
            producer.send(record);
            producer.flush();
        }

        producer.close();
    }

    public void produce(int messageCount, Duration gap) {
        registerShutdownHook();
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(props);

            int topicSelectionCounter = 0;
            int globalCounter = 1;
            HashMap<String, Integer> topicCounters = new HashMap<>();
            for(String topic : topics)
                topicCounters.put(topic, 1);

            while (!isCancelled && globalCounter <= messageCount) {
                String topic = topics.get(topicSelectionCounter % topics.size());
                int msgCount = msgCounts.get(topicSelectionCounter % topics.size());

                for(int i=0; i<msgCount; i++) {
                    if(globalCounter > messageCount)
                        break;

                    int topicCounter = topicCounters.get(topic);
                    topicCounters.put(topic, topicCounter + 1);

                    String msg = MessageFormat.format("{0,number,#},{1,number,#}", globalCounter, topicCounter);

                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "NoKey", msg);

                    RecordMetadata metadata = producer.send(record).get();
                    if (metadata.hasOffset()) {
                        print(MessageFormat.format("Topic: {0}, TC: {1}, GC: {2}",
                                metadata.topic().substring(metadata.topic().lastIndexOf("/")+1, metadata.topic().indexOf("_")),
                                topicCounter,
                                globalCounter));
                    } else {
                        print("Could not send message");
                    }

                    globalCounter++;
                    waitFor((int)gap.toMillis());
                }

                topicSelectionCounter++;
            }

            producer.flush();
            tryClose(producer);
        } catch (Exception e) {
            e.printStackTrace();
            tryClose(producer);
        } finally {
            isCompleted = true;
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                isCancelled = true;

                while (!isCompleted)
                    waitFor(100);
            }
        });
    }

    private void waitFor(int milliseconds) {
        if(milliseconds == 0)
            return;

        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void print(String text) {
        System.out.println(Instant.now() + " : Producer : " + text);
    }

    private void tryClose(Producer producer) {
        try {
            producer.close(Duration.ofSeconds(60));
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}