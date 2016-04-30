package edu.sjsu.cmpe239.moonshot;


import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.stereotype.Component;
import scala.Tuple2;


import com.google.common.io.Files;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.util.*;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;


@Component
public class KafkaStreaming {
    static Graph graph = new SingleGraph("Tutorial 1");

    private static final class STREAM_KAFKA_MESSAGES implements Function<Tuple2<String,String>, String> {
        public String call(Tuple2<String, String> message) throws IOException{
            String jsonl = "";
            jsonl = message._2.toString();
            char c = jsonl.charAt(0);
            //System.out.println("++++++++++++++++++++++++");
           // System.out.println(message._2.toString());
            //System.out.println("++++++++++++++++++++++++");
            List<String> a = new ArrayList();
            String result = "";
            if(c == '{') {
                JsonParser parser = new JsonParser();
                JsonObject rootObj = parser.parse(jsonl).getAsJsonObject();
                //System.out.println("********************************");
                if (rootObj.get("text") != null) {
                    result = rootObj.get("text").toString();
                    //System.out.println(result);
                    a.add(result);
                }else
                {
                    //System.out.println("NULLLLLLLLLLLLLLLLLLLLLLLLLLLL");
                }

            }

            //System.out.println(a);
            //graph = new SingleGraph("Tutorial 1");
            /*graph.addNode(message._2());
            graph.addNode("B" );
            graph.addNode("C" );
            graph.addEdge(message._2() + "B", message._2(), "B");
            graph.addEdge("BC", "B", "C");
            graph.addEdge("C"+message._2(), "C", message._2());
            graph.display();
            */
            return result;
        }
    };

    private static final class STREAM_Lines implements Function<Tuple2<String,String>, String>{
        public String call(Tuple2<String, String> message) {
            return message._2();
       }
    };


    private static final class splitWords implements FlatMapFunction<String, String>{
        public Iterable<String> call(String x) {
            return Arrays.asList(x.split(" "));
        }
    };

    private static final class wordMapper implements PairFunction<String, String, Integer>{
        public Tuple2<String, Integer> call(String x) {
            return new Tuple2(x, 1);
        }
    };

    private static final class WordCountReducer implements Function2<Integer, Integer, Integer>{
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    };

    /*public JavaPairDStream<String, Integer> splitWords(JavaDStream<String> lines) {
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) { return Lists.newArrayList(x.split(" ")); }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) { return new Tuple2<>(s, 1); }
                }).reduceByKey((i1, i2) -> i1 + i2);

        return wordCounts;
    }*/

    public void SparkStreaming(List<String> topicList, int numberThreads) throws IOException{
       /* if (args.length < 4) {
            System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }*/


        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //The appName parameter is a name for your application to show on the cluster UI. master is a Spark, Mesos or
        // YARN cluster URL, or a special “local[*]” string to run in local mode.
        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaSparkStreaming").setMaster("local[4]");

        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(3000));

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topicList) {
            topicMap.put(topic, numberThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, "localhost:2181", "tweet", topicMap);
        JavaDStream<String> line = messages.map(new STREAM_KAFKA_MESSAGES());
        JavaDStream<String> words = line.flatMap(new splitWords());

        JavaPairDStream<String, Integer> wordCount = words.mapToPair(new wordMapper()).reduceByKey(new WordCountReducer());


        wordCount.print();
        jssc.start();
        jssc.awaitTermination();

        //ObjectMapper mapper = new ObjectMapper();
        //Tweet t = mapper.readValue(line.toString(), Tweet.class);

        //String jsonl = "";
        //jsonl = line.toString();
        //char c = jsonl.charAt(0);

        //pairs.values();
        //System.out.println("*");


       /* if(c == '{')
        {
            //JsonReader.setLenient(true);
            JsonElement jelement = new JsonParser().parse(jsonl);
            JsonObject jobject = jelement.getAsJsonObject();
            //jobject = jobject.getAsJsonObject("data");
            //JsonArray jarray = jobject.getAsJsonArray("translations");
            //jobject = jarray.get(0).getAsJsonObject();



            System.out.println("********************************");
            line.print();
            //String result = jobject.get("text").toString();
            System.out.println("********************************");
            jssc.start();
            jssc.awaitTermination();
        }
        else
        {
            System.out.println("********************************");
            line.print();
            System.out.println("********************************");

            jssc.start();
            jssc.awaitTermination();
        }*/
    }
}
