package com.jackvanlightly.multitopicordering.pulsar;

import org.apache.pulsar.client.api.*;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PulsarSequenceProducer {
    private boolean isCancelled;
    private boolean isCompleted;

    Map<String,Producer<String>> producers;
    PulsarClient client;
    List<String> topics;
    List<Integer> msgCounts;

    public PulsarSequenceProducer(List<String> topics, List<Integer> msgCounts) {
        this.topics = topics;

        if(msgCounts == null)
            this.msgCounts = topics.stream().map(x -> 1).collect(Collectors.toList());
        else
            this.msgCounts = msgCounts;
        producers = new HashMap<>();

        try {
            client = PulsarClient.builder()
                    .serviceUrl(PulsarConnection.ServiceUrl)
                    .build();

            for (String topic : topics) {
                Producer<String> stringProducer = client.newProducer(Schema.STRING)
                        .topic(topic)
                        .blockIfQueueFull(true)
                        .create();

                producers.put(topic, stringProducer);
            }
        }
        catch(PulsarClientException e)
        {
            e.printStackTrace();
            throw new RuntimeException("Failed starting client and producers", e);
        }
    }

//    public void produceEqualDistribution(int messageCount) {
//        registerShutdownHook();
//        try {
//            int globalCounter = 1;
//            HashMap<String, Integer> topicCounters = new HashMap<>();
//            for(String topic : topics)
//                topicCounters.put(topic, 1);
//
//            while (!isCancelled && globalCounter <= messageCount) {
//                String topic = topics.get(globalCounter % topics.size());
//                int topicCounter = topicCounters.get(topic);
//                topicCounters.put(topic, topicCounter+1);
//
//                String msg = MessageFormat.format("{0},{1}", globalCounter, topicCounter);
//
//                Producer<String> producer = producers.get(topic);
//                MessageId msgId = producer.newMessage()
//                        .key("x")
//                        .value(msg)
//                        .send();
//
//                print(MessageFormat.format("Topic: {0}, TC: {1}, GC: {2}",
//                        topic.substring(topic.lastIndexOf("/")+1, topic.indexOf("_")),
//                        globalCounter,
//                        topicCounter));
//
//                globalCounter++;
//            }
//
//            tryClose();
//        } catch (Exception e) {
//            e.printStackTrace();
//            tryClose();
//        } finally {
//            isCompleted = true;
//        }
//    }

    public void produce(int messageCount, Duration gap) {
        registerShutdownHook();
        try {
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

                    Producer<String> producer = producers.get(topic);
                    MessageId msgId = producer.newMessage()
                            .key("x")
                            .value(msg)
                            .send();

                    print(MessageFormat.format("Topic: {0}, TC: {1}, GC: {2}",
                            topic.substring(topic.lastIndexOf("/")+1, topic.indexOf("_")),
                            topicCounter,
                            globalCounter));

                    globalCounter++;

                    waitFor((int)gap.toMillis());
                }

                topicSelectionCounter++;
            }

            tryClose();
        } catch (Exception e) {
            e.printStackTrace();
            tryClose();
        } finally {
            isCompleted = true;
        }
    }

    public void declareTopics() {
        try {
            for (String topic : topics) {
                String msg = "start";
                Producer<String> producer = producers.get(topic);
                producer.newMessage()
                        .key("x")
                        .value(msg)
                        .send();
                producer.close();
            }
        }
        catch(PulsarClientException e) {
            e.printStackTrace();
        }

    }

    public void produceWithConfigurationDistributionWithSeqNos(int messageCount, Duration gap) {
        registerShutdownHook();
        try {
            int topicSelectionCounter = 0;
            int globalCounter = 1;
            HashMap<String, Integer> topicCounters = new HashMap<>();
            for(String topic : topics)
                topicCounters.put(topic, 1);

            while (!isCancelled && globalCounter <= messageCount) {
                String topic = topics.get(topicSelectionCounter % topics.size());
                int msgCount = msgCounts.get(topicSelectionCounter % topics.size());

                for(int i=0; i<msgCount; i++) {
                    int topicCounter = topicCounters.get(topic);
                    topicCounters.put(topic, topicCounter + 1);

                    String msg = MessageFormat.format("{0},{1}", globalCounter, topicCounter);

                    Producer<String> producer = producers.get(topic);
                    MessageId msgId = producer.newMessage()
                            .key("x")
                            .value(msg)
                            .sequenceId(globalCounter)
                            .send();

                    print(MessageFormat.format("Topic: {0}, Global: {1} Topic: {2}",
                            topic,
                            globalCounter,
                            topicCounter));

                    globalCounter++;

                    waitFor((int)gap.toMillis());
                }

                topicSelectionCounter++;
            }

            tryClose();
        } catch (Exception e) {
            e.printStackTrace();
            tryClose();
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

    private void tryClose() {
        try {
            for(Producer<String> producer : producers.values())
                producer.close();

            client.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
