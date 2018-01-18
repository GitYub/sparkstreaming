package com.yxy.sparkstreaming;

import com.yxy.sparkstreaming.service.DataAnalysis;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.*;

/**
 * @Author: Xinyu Yu
 * @Description: SparkStreaming 通过Direct Approach接收来自Kafka的数据流
 * @Date: 10:09 2018/1/17
 */
public class BAStreamingApp {
    public static void main(String[] args) throws InterruptedException {

        if (args.length != 2) {
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];

        SparkConf sparkConf = new SparkConf();//.setAppName("BAStreamingApp");//.setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(11));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        DataAnalysis.getData(lines);
        //lines.print();

//        if (args.length != 4) {
//            System.exit(1);
//        }
//
//        String zkQuorum = args[0];
//        String groupid = args[1];
//        String[] topicMaps = args[2].split(",");
//        String numThreads = args[3];
//
//        SparkConf sparkConf = new SparkConf();//.setAppName("BAStreamingApp");//.setMaster("local[2]");
//        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
//
//        Map<String, Integer> topicMap = new HashMap<>();
//        for (String a : topicMaps) {
//            topicMap.put(a, Integer.valueOf(numThreads));
//        }
//
//        JavaPairReceiverInputDStream<String, String> javaPairReceiverInputDStream =
//                KafkaUtils.createStream(jssc, zkQuorum, groupid, topicMap);
//
//        JavaDStream<String> lines =
//                javaPairReceiverInputDStream.map(Tuple2::_2);
//        lines.print();
//
//
        jssc.start();
        jssc.awaitTermination();
    }
}
