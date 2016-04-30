package edu.sjsu.cmpe239.moonshot;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Arrays;

public class WordCountExample {
    public static void main(String[] args) throws Exception {
        String inputFile = "src/main/resources/input_file.txt";
        String outputFile = "src/main/resources/output_file";

        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("edu.sjsu.cmpe239.WordCountExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        // 3 ways to invoke a function
        //1. Embedded Invoking
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) {
                return Arrays.asList(x.split(" "));
            }
        });

        System.out.println("Approach 1: " + words.take(3));


        // 2. Instance - Transform into word and count.
        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String x) {
                return new Tuple2(x, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });

        System.out.println("Approach 2: " + counts.take(2));

        //Make sure you delete the previous output file before running the instance
        counts.saveAsTextFile(outputFile);
    }
}
