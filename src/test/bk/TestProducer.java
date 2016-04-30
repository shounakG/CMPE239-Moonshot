package edu.sjsu.cmpe239.moonshot;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

//Twitter Imports

public class TestProducer {
    private static Producer<Integer, String> producer;
    private final Properties properties = new Properties();

    public TestProducer() {
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        producer = new Producer<>(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
        new TestProducer();
        String topic = "test";
        String msg = "a";
        KeyedMessage<Integer, String> data = new KeyedMessage<>(topic, msg);
        producer.send(data);
        producer.close();
    }
}