package com.jackvanlightly.multitopicordering;

import com.jackvanlightly.multitopicordering.kafka.KafkaSequenceConsumer;
import com.jackvanlightly.multitopicordering.kafka.KafkaSequenceMultiConsumer;
import com.jackvanlightly.multitopicordering.kafka.KafkaSequenceProducer;
import com.jackvanlightly.multitopicordering.pulsar.PulsarSequenceConsumer;
import com.jackvanlightly.multitopicordering.pulsar.PulsarSequenceMultiConsumer;
import com.jackvanlightly.multitopicordering.pulsar.PulsarSequenceProducer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
//        kafkaTailingTestWithZeroProc(1,1);
//        kafkaTailingTestWithJitter(1, 1);
//        kafkaTailingTestFastProducer(1, 1);
//        kafkaTailingTestWithZeroProc(10,1);
//        kafkaTailingTestWithJitter(10, 1);
//        kafkaTailingTestFastProducer(10, 1);
//        kafkaTimeTravelTest(1,1);
        kafkaTimeTravelTestMultiConsumer(1,1);


//        pulsarTailingTestWithZeroProc(1, 1); // Pulsar Case 1
//        pulsarTailingTestWithInterval(1, 1); // Pulsar Case 2
//        pulsarTailingTestWithJitter(1, 1); // Pulsar Case 3
//        pulsarTailingTestFastProducer(1,1); // Pulsar Case 4
//        pulsarTailingTestWithZeroProc(10, 1); // Pulsar Case 5
//        pulsarTailingTestWithInterval(10, 1); // Pulsar Case 6
//        pulsarTailingTestWithJitter(10, 1); // Pulsar Case 7
//        pulsarTailingTestFastProducer(10,1); // Pulsar Case 8
//        pulsarTimeTravelTest(1, 1); // Pulsar Case 9
//        pulsarTimeTravelTest(10, 1); // Pulsar Case 10

    }

    private static void kafkaTailingTestWithZeroProc(int topic1Ratio, int topic2Ratio) {
        String suffix = getSuffix();
        List<String> topics = Arrays.asList("topic1"+suffix, "topic2"+suffix);

        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio); // controls if it is round-robin or other ratio
        KafkaSequenceProducer orderProducer = new KafkaSequenceProducer(topics, msgCounts);
        orderProducer.declareTopics();
        KafkaSequenceConsumer consumer = new KafkaSequenceConsumer(1, topics, false);
        executor.submit(() -> consumer.consume(Duration.ZERO, Duration.ZERO, false));

        while(!consumer.isAssigned())
            waitFor(100);

        executor.submit(() -> orderProducer.produce(100, Duration.ZERO));

        executor.shutdown();
    }

    private static void kafkaTailingTestWithJitter(int topic1Ratio, int topic2Ratio) {
        String suffix = getSuffix();
        List<String> topics = Arrays.asList("topic1"+suffix, "topic2"+suffix);

        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio); // controls if it is round-robin or other ratio
        KafkaSequenceProducer orderProducer = new KafkaSequenceProducer(topics, msgCounts);
        orderProducer.declareTopics();
        KafkaSequenceConsumer consumer = new KafkaSequenceConsumer(1, topics, false);
        executor.submit(() -> consumer.consume(Duration.ofMillis(1), Duration.ofMillis(50), true));

        while(!consumer.isAssigned())
            waitFor(100);

        executor.submit(() -> orderProducer.produce(100, Duration.ofMillis(25)));

        executor.shutdown();
    }

    private static void kafkaTailingTestFastProducer(int topic1Ratio, int topic2Ratio) {
        String suffix = getSuffix();
        List<String> topics = Arrays.asList("topic1"+suffix, "topic2"+suffix);

        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio); // controls if it is round-robin or other ratio
        KafkaSequenceProducer orderProducer = new KafkaSequenceProducer(topics, msgCounts);
        orderProducer.declareTopics();
        KafkaSequenceConsumer consumer = new KafkaSequenceConsumer(1, topics, false);
        executor.submit(() -> consumer.consume(Duration.ofMillis(100), Duration.ofMillis(100), false));

        while(!consumer.isAssigned())
            waitFor(100);

        executor.submit(() -> orderProducer.produce(100, Duration.ofMillis(20)));

        executor.shutdown();
    }

    private static void kafkaTimeTravelTest(int topic1Ratio, int topic2Ratio) {
        String suffix = getSuffix();
        List<String> topics = Arrays.asList("topic1"+suffix, "topic2"+suffix);
        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio);
        ExecutorService executor = Executors.newFixedThreadPool(5);

        KafkaSequenceProducer orderProducer = new KafkaSequenceProducer(topics, msgCounts);
        orderProducer.produce(100, Duration.ofMillis(10));

        KafkaSequenceConsumer consumer = new KafkaSequenceConsumer(1, topics, true);
        executor.submit(() -> consumer.consume(Duration.ofMillis(0), Duration.ofMillis(0), false));

        executor.shutdown();
    }

    private static void kafkaTimeTravelTestMultiConsumer(int topic1Ratio, int topic2Ratio) {
        String suffix = getSuffix();
        List<String> topics = Arrays.asList("topic1"+suffix, "topic2"+suffix);
        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio);
        ExecutorService executor = Executors.newFixedThreadPool(20);

        KafkaSequenceProducer orderProducer = new KafkaSequenceProducer(topics, msgCounts);
        orderProducer.produce(100, Duration.ofMillis(1));

        KafkaSequenceMultiConsumer consumer = new KafkaSequenceMultiConsumer(topics, true);
        consumer.initialize();
        executor.submit(() -> consumer.consume());

        executor.shutdown();
    }

    private static void pulsarTailingTestWithZeroProc(int topic1Ratio, int topic2Ratio) {
        String prefix = "persistent://vanlightly/cluster-1/ns1/";
        String suffix = getSuffix();
        List<String> topics = Arrays.asList(prefix+"topic1"+suffix, prefix+"topic2"+suffix);
        ExecutorService executor = Executors.newFixedThreadPool(5);

        PulsarSequenceConsumer consumer = new PulsarSequenceConsumer(1, topics, false);
        executor.submit(() -> consumer.consume(Duration.ZERO, Duration.ZERO, false));

        waitFor(100);


        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio); // controls if it is round-robin or other ratio
        PulsarSequenceProducer orderProducer = new PulsarSequenceProducer(topics, msgCounts);
        executor.submit(() -> orderProducer.produce(100, Duration.ZERO));

        executor.shutdown();
    }

    private static void pulsarTailingTestWithInterval(int topic1Ratio, int topic2Ratio) {
        String prefix = "persistent://vanlightly/cluster-1/ns1/";
        String suffix = getSuffix();
        List<String> topics = Arrays.asList(prefix+"topic1"+suffix, prefix+"topic2"+suffix);
        ExecutorService executor = Executors.newFixedThreadPool(5);

        PulsarSequenceConsumer consumer = new PulsarSequenceConsumer(1, topics, false);
        executor.submit(() -> consumer.consume(Duration.ofMillis(10), Duration.ofMillis(10), false));

        waitFor(100);

        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio); // controls if it is round-robin or other ratio
        PulsarSequenceProducer orderProducer = new PulsarSequenceProducer(topics, msgCounts);
        executor.submit(() -> orderProducer.produce(100, Duration.ofMillis(10)));

        executor.shutdown();
    }

    private static void pulsarTailingTestWithJitter(int topic1Ratio, int topic2Ratio) {
        String prefix = "persistent://vanlightly/cluster-1/ns1/";
        String suffix = getSuffix();
        List<String> topics = Arrays.asList(prefix+"topic1"+suffix, prefix+"topic2"+suffix);
        ExecutorService executor = Executors.newFixedThreadPool(5);

        PulsarSequenceConsumer consumer = new PulsarSequenceConsumer(1, topics, false);
        executor.submit(() -> consumer.consume(Duration.ofMillis(1), Duration.ofMillis(50), true));

        waitFor(100);

        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio); // controls if it is round-robin or other ratio
        PulsarSequenceProducer orderProducer = new PulsarSequenceProducer(topics, msgCounts);
        executor.submit(() -> orderProducer.produce(100, Duration.ofMillis(25)));

        executor.shutdown();
    }

    private static void pulsarTailingTestFastProducer(int topic1Ratio, int topic2Ratio) {
        String prefix = "persistent://vanlightly/cluster-1/ns1/";
        String suffix = getSuffix();
        List<String> topics = Arrays.asList(prefix+"topic1"+suffix, prefix+"topic2"+suffix);
        ExecutorService executor = Executors.newFixedThreadPool(5);

        // set receiveQueueSize to ten order to better test a fast producer
        PulsarSequenceConsumer consumer = new PulsarSequenceConsumer(1, topics, false, 10);
        executor.submit(() -> consumer.consume(Duration.ofMillis(100), Duration.ofMillis(100), false));

        waitFor(5000);

        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio); // controls if it is round-robin or other ratio
        PulsarSequenceProducer orderProducer = new PulsarSequenceProducer(topics, msgCounts);
        executor.submit(() -> orderProducer.produce(100, Duration.ofMillis(1)));

        executor.shutdown();
    }

    private static void pulsarTimeTravelTest(int topic1Ratio, int topic2Ratio) {
        String prefix = "persistent://vanlightly/cluster-1/ns1/";
        String suffix = getSuffix();
        List<String> topics = Arrays.asList(prefix+"topic1"+suffix, prefix+"topic2"+suffix);

        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio);
        PulsarSequenceProducer orderProducer = new PulsarSequenceProducer(topics, msgCounts);
        orderProducer.produce(100, Duration.ofMillis(1));

        PulsarSequenceConsumer consumer = new PulsarSequenceConsumer(1, topics, true);
        executor.submit(() -> consumer.consume(Duration.ofMillis(0), Duration.ofMillis(0), false));

        executor.shutdown();
    }

    private static void pulsarTimeTravelMultiConsumer(int topic1Ratio, int topic2Ratio) {
        String prefix = "persistent://vanlightly/cluster-1/ns2/";
        String suffix = getSuffix();
        List<String> topics = Arrays.asList(prefix+"topic1"+suffix, prefix+"topic2"+suffix);

        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<Integer> msgCounts = Arrays.asList(topic1Ratio, topic2Ratio); // controls if it is round-robin or other ratio
        PulsarSequenceProducer orderProducer = new PulsarSequenceProducer(topics, msgCounts);
        orderProducer.produce(100, Duration.ofMillis(100));

        PulsarSequenceMultiConsumer consumer = new PulsarSequenceMultiConsumer(topics);
        consumer.initialize(true);
        executor.submit(() -> consumer.consume());

        executor.shutdown();
    }

    private static void waitFor(int milliseconds) {
        try
        {
            Thread.sleep(milliseconds);
        }
        catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
    }

    private static String getSuffix() {
        Random rand = new Random();
        return "_" + String.valueOf(rand.nextInt(9999));
    }
}
