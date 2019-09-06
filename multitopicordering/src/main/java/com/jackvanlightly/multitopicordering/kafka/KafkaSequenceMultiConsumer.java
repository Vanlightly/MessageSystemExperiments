package com.jackvanlightly.multitopicordering.kafka;

import com.jackvanlightly.multitopicordering.pulsar.NextMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class KafkaSequenceMultiConsumer {
    private boolean isCancelled;
    private boolean isCompleted;

    Properties props;

    List<String> consumerIds;
    List<KafkaConsumer<String, String>> consumers;
    List<String> topics;
    List<BlockingQueue<BufferedMessage>> buffers;

    public KafkaSequenceMultiConsumer(List<String> topics, boolean fromBeginning) {
        this.topics = topics;
        consumerIds = new ArrayList<>();
        consumers = new ArrayList<>();
        buffers = new ArrayList<>();

        initProps(fromBeginning);
    }

    private void initProps(boolean fromBeginning) {
        props = new Properties();
        props.put("bootstrap.servers", KafkaConnection.BootstrapServers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");

        if(fromBeginning)
            props.put("auto.offset.reset", "earliest");
    }

    public void initialize() {
        int ctr = 1;
        for(String topic : topics) {
            consumerIds.add("C" + ctr);
            ctr++;

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));

            consumers.add(consumer);
            buffers.add(new LinkedBlockingDeque<>(1000));
        }
    }

    public void consume() {
        registerShutdownHook();

        // run consumer threads
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(topics.size());
        for(int i=0; i<topics.size(); i++) {
            KafkaConsumer<String,String> consumer = consumers.get(i);
            String consumerId = consumerIds.get(i);
            BlockingQueue<BufferedMessage> buffer = buffers.get(i);

            consumerExecutor.submit(() -> runConsumer(consumer, consumerId, buffer));
        }

        // give the consumers time to start up
        waitFor(10000);

        int lastGlobalCounter = 0;
        Map<String,Integer> lastTopicCounterMap = new HashMap<>();

        while (!isCancelled) {
            BufferedMessage nextMsg = getNextMessage();
            if(nextMsg != null) {
                if(nextMsg.getRecord().value().equals("start"))
                    continue;

                lastGlobalCounter = processMessage(nextMsg, lastTopicCounterMap, lastGlobalCounter);
            }
            else {
                print("MAIN", "No messages in any buffers");
                waitFor(1000);
            }
        }

        tryClose();
    }

    private void runConsumer(KafkaConsumer<String,String> consumer, String consumerId, BlockingQueue<BufferedMessage> buffer) {
        try {
            while (!isCancelled) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        BufferedMessage msg = new BufferedMessage(record, consumer, consumerId);
                        while (!isCancelled && !buffer.offer(msg))
                            waitFor(100);

                        if (isCancelled)
                            break;
                    }
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private BufferedMessage getNextMessage() {
        long minTimestamp = Long.MAX_VALUE;
        int targetIndex = -1;
        for(int i=0; i<topics.size(); i++) {
            BufferedMessage msg = buffers.get(i).peek();
            if(msg != null) {
                long timestamp = msg.getRecord().timestamp();
                if (timestamp < minTimestamp) {
                    minTimestamp = timestamp;
                    targetIndex = i;
                }
            }
        }

        if(targetIndex == -1)
            return null;

        return buffers.get(targetIndex).poll();
    }

    private int processMessage(BufferedMessage nextMsg,
                               Map<String,Integer> lastTopicCounterMap,
                               int lastGlobalCounter) {
        ConsumerRecord<String,String> record = nextMsg.getRecord();
        KafkaConsumer<String,String> consumer = nextMsg.getConsumer();
        String consumerId = nextMsg.getConsumerId();

        int globalCounter = 0;
        try {
            String seqNoKey = record.topic()+record.partition();
            globalCounter = Integer.valueOf(record.value().split(",")[0]);
            int topicCounter = Integer.valueOf(record.value().split(",")[1]);

            int lastTopicCounter = 0;
            if (lastTopicCounterMap.containsKey(seqNoKey))
                lastTopicCounter = lastTopicCounterMap.get(seqNoKey);

            String topicOrderLabel = "TOPIC: " + getOrderingText(lastTopicCounter, topicCounter);
            String globalOrderLabel = "GLOBAL: " + getOrderingText(lastGlobalCounter, globalCounter);

            print(consumerId, MessageFormat.format("Topic: {0}, TC: {1} {2}, GC: {3} {4}",
                    record.topic().substring(record.topic().lastIndexOf("/")+1, record.topic().indexOf("_")),
                    topicCounter,
                    topicOrderLabel,
                    globalCounter,
                    globalOrderLabel));

            lastTopicCounterMap.put(seqNoKey, topicCounter);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return globalCounter;
    }

    private String getOrderingText(int last, int current) {
        if(last + 1 < current)
            return "JUMP FORWARD " + (current - (last + 1));
        else if(last + 1 > current)
            return "JUMP BACKWARDS " + (last - current);
        else
            return "OK";
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
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void print(String conId, String text) {
        System.out.println(Instant.now() + " : Consumer : " + text);
    }

    private void tryClose() {
        try {
            for(KafkaConsumer<String,String> consumer : consumers)
                consumer.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
