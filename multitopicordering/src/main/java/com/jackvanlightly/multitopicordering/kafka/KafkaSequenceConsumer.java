package com.jackvanlightly.multitopicordering.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class KafkaSequenceConsumer implements ConsumerRebalanceListener {
    private boolean isCancelled;
    private boolean isCompleted;
    private boolean isAssigned;


    private String consumerId;
    private List<String> topics;
    private Properties props;
    Random rand = new Random();

    public KafkaSequenceConsumer(int consumerId, List<String> topics, boolean fromBeginning) {
        this.topics = topics;
        this.consumerId = "C" + consumerId;
        initProps(fromBeginning);
    }

    public KafkaSequenceConsumer(int consumerId, List<String> topics, boolean fromBeginning, Properties customProps) {
        this(consumerId, topics, fromBeginning);

        for(Map.Entry prop : customProps.entrySet())
            props.put(prop.getKey(), prop.getValue());
    }

    public boolean isAssigned() {
        return isAssigned;
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

    public void consume(Duration minGap, Duration maxGap, boolean withExtraJitter) {
        registerShutdownHook();
        KafkaConsumer<String, String> consumer = null;

        try {
            print("Subscribing");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(topics, this);

            int lastGlobalCounter = 0;
            Map<String,Integer> lastTopicCounterMap = new HashMap<>();

            while (!isCancelled) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    if(record.value().equals("start"))
                        continue;

                    String seqNoKey = record.topic()+record.partition();
                    int globalCounter = Integer.valueOf(record.value().split(",")[0]);
                    int topicCounter = Integer.valueOf(record.value().split(",")[1]);

                    int lastTopicCounter = 0;
                    if(lastTopicCounterMap.containsKey(seqNoKey))
                        lastTopicCounter = lastTopicCounterMap.get(seqNoKey);

                    String topicOrderLabel = getOrderingText(lastTopicCounter, topicCounter);
                    String globalOrderLabel = getOrderingText(lastGlobalCounter, globalCounter);

                    print(MessageFormat.format("Topic: {0}, TC: {1} {2}, GC: {3} {4}",
                            record.topic().substring(record.topic().lastIndexOf("/")+1, record.topic().indexOf("_")),
                            topicCounter,
                            topicOrderLabel,
                            globalCounter,
                            globalOrderLabel));

                    lastGlobalCounter = globalCounter;
                    lastTopicCounterMap.put(seqNoKey, topicCounter);

                    waitFor(minGap, maxGap, withExtraJitter);
                }
            }

            tryClose(consumer);
        }
        catch (Exception e) {
            e.printStackTrace();
            tryClose(consumer);
        }
        finally {
            isCompleted = true;
            print("Consumer exited");
        }
    }

    private String getOrderingText(int last, int current) {
        if(last + 1 < current)
            return "JUMP FORWARD " + (current - (last + 1));
        else if(last + 1 > current)
            return "JUMP BACKWARDS " + (last - current);
        else
            return "OK";
    }

//    public void consumeTwoConsumers() {
//        registerShutdownHook();
//        KafkaConsumer<String, String> consumer1 = null;
//        KafkaConsumer<String, String> consumer2 = null;
//
//        try {
//            consumer1 = new KafkaConsumer<>(props);
//            consumer1.subscribe(Arrays.asList(topics.get(0)), this);
//            consumer2 = new KafkaConsumer<>(props);
//            consumer2.subscribe(Arrays.asList(topics.get(1)), this);
//
//            while (!isCancelled) {
//                ConsumerRecords<String, String> records1 = consumer1.poll(Duration.ofMillis(1000));
//                ConsumerRecords<String, String> records2 = consumer2.poll(Duration.ofMillis(1000));
//
//                for (ConsumerRecord<String, String> record : records1) {
//                    print("C1", MessageFormat.format("Topic: {0}, Partition: {1}, {2}",
//                            record.topic(),
//                            record.partition(),
//                            record.value()));
//                }
//
//                for (ConsumerRecord<String, String> record : records2) {
//                    print("C2", MessageFormat.format("Topic: {0}, Partition: {1}, {2}",
//                            record.topic(),
//                            record.partition(),
//                            record.value()));
//                }
//            }
//
//            tryClose(consumer1);
//            tryClose(consumer2);
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//            tryClose(consumer1);
//            tryClose(consumer2);
//        }
//        finally {
//            isCompleted = true;
//            print("Producer exited");
//        }
//    }



    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                isCancelled = true;

                while(!isCompleted)
                    waitFor(100);

                System.out.println("Producer thread exited");
            }
        });
    }

    private void waitFor(Duration minGap, Duration maxGap, boolean withExtraJitter) {
        if(withExtraJitter && rand.nextInt(100) < 5)
            waitFor(200);

        if(maxGap.toMillis() > 0) {
            if(minGap.toMillis() == maxGap.toMillis()) {
                waitFor((int) minGap.toMillis());
            }
            else {
                int ms = rand.nextInt((int) maxGap.toMillis() - (int) minGap.toMillis()) + (int) minGap.toMillis();
                waitFor(ms);
            }
        }
    }

    private void waitFor(int milliseconds) {
        try
        {
            Thread.sleep(milliseconds);
        }
        catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        print("Partitions revoked at " + Instant.now());
        isAssigned = false;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        for(TopicPartition tp : collection) {
            print("Topic: " + tp.topic() + " Partition: " + tp.partition() + " assigned at " + Instant.now());
            isAssigned = true;
        }

    }

    private void print(String text) {
        System.out.println(Instant.now() + " : Consumer : " + text);
    }

    private void print(String conId, String text) {
        System.out.println(Instant.now() + " : Consumer " + conId + " : " + text);
    }

    private void tryClose(KafkaConsumer<String, String> consumer) {
        try {
            consumer.close(Duration.ofSeconds(60));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
