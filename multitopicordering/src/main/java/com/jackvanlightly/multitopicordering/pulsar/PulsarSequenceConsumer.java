package com.jackvanlightly.multitopicordering.pulsar;

import org.apache.pulsar.client.api.*;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class PulsarSequenceConsumer {
    private boolean isCancelled;
    private boolean isCompleted;

    private String consumerId;
    Consumer consumer;
    PulsarClient client;
    List<String> topics;
    Random rand = new Random();

    public PulsarSequenceConsumer(int consumerId, List<String> topics, boolean fromBeginning) {
        // default receiverQueueSize is 1000 anyway
        this(consumerId, topics, fromBeginning, 1000);
    }

    public PulsarSequenceConsumer(int consumerId, List<String> topics, boolean fromBeginning, int receiverQueueSize) {
        this.topics = topics;
        this.consumerId = "C" + consumerId;

        try {
            client = PulsarClient.builder()
                    .serviceUrl(PulsarConnection.ServiceUrl)
                    .build();

            SubscriptionInitialPosition startPos = fromBeginning ? SubscriptionInitialPosition.Earliest : SubscriptionInitialPosition.Latest;

            print("Subscribing");
            consumer = client.newConsumer()
                    .topics(topics)
                    .subscriptionInitialPosition(startPos)
                    .subscriptionName(UUID.randomUUID().toString())
                    .receiverQueueSize(receiverQueueSize)
                    .subscribe();
        }
        catch(PulsarClientException e)
        {
            e.printStackTrace();
            throw new RuntimeException("Failed starting client and producers", e);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void consume(Duration minGap, Duration maxGap, boolean withExtraJitter) {
        registerShutdownHook();
        try {
            print("Consuming");
            int lastGlobalCounter = 0;
            Map<String,Integer> lastTopicCounterMap = new HashMap<>();

            while (!isCancelled) {
                // Wait for a message
                Message msg = consumer.receive();


                try {

                    String payload = new String(msg.getData());

                    String seqNoKey = msg.getTopicName();
                    int globalCounter = Integer.valueOf(payload.split(",")[0]);
                    int topicCounter = Integer.valueOf(payload.split(",")[1]);

                    int lastTopicCounter = 0;
                    if(lastTopicCounterMap.containsKey(seqNoKey))
                        lastTopicCounter = lastTopicCounterMap.get(seqNoKey);

                    String topicOrderLabel = getOrderingText(lastTopicCounter, topicCounter);
                    String globalOrderLabel = getOrderingText(lastGlobalCounter, globalCounter);

                    print(consumerId, MessageFormat.format("Topic: {0}, TC: {1} {2}, GC: {3} {4}",
                            msg.getTopicName().substring(msg.getTopicName().lastIndexOf("/")+1, msg.getTopicName().indexOf("_")),
                            topicCounter,
                            topicOrderLabel,
                            globalCounter,
                            globalOrderLabel));

//                    print(consumerId, MessageFormat.format("Topic: {0}, TC: {1} {2}, GC: {3} {4} {5}",
//                            msg.getTopicName().substring(msg.getTopicName().lastIndexOf("/")+1, msg.getTopicName().indexOf("_")),
//                            topicCounter,
//                            topicOrderLabel,
//                            globalCounter,
//                            globalOrderLabel,
//                            msg.getPublishTime()));

                    lastGlobalCounter = globalCounter;
                    lastTopicCounterMap.put(seqNoKey, topicCounter);

                    // Acknowledge the message so that it can be deleted by the message broker
                    consumer.acknowledge(msg);
                    waitFor(minGap, maxGap, withExtraJitter);
                } catch (Exception e) {
                    e.printStackTrace();
                    // Message failed to process, redeliver later
                    consumer.negativeAcknowledge(msg);
                }
            }
        }
        catch(PulsarClientException e) {
            e.printStackTrace();

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
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void print(String text) {
        System.out.println(Instant.now() + " : Consumer " + consumerId + " : " + text);
    }

    private void print(String conId, String text) {
        System.out.println(Instant.now() + " : Consumer : " + text);
    }

    private void tryClose() {
        try {
            consumer.close();
            client.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
