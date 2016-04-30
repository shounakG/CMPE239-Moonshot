package edu.sjsu.cmpe239.moonshot;

import java.util.ArrayList;
import java.util.List;

public class SparkStreamMain {
    public static void main(String[] args) throws Exception {
        KafkaStreaming kafkaStream = new KafkaStreaming();
        List<String> topicList = new ArrayList<>();
        topicList.add("tweet");
        kafkaStream.SparkStreaming(topicList, 2);
    }
}
