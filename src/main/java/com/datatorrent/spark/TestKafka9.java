//package com.datatorrent.spark;
//
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//
//import org.apache.spark.sql.streaming.StreamingQueryException;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//import org.apache.spark.streaming.Milliseconds;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//
//import kafka.serializer.StringDecoder;
//
///**
// * Created by bhupesh on 8/2/18.
// */
//public class SparkTestApp
//{
//  public static void main(String[] args) throws StreamingQueryException, InterruptedException
//  {
//    JavaStreamingContext jsc = new JavaStreamingContext("spark://earth:7077", "Test", Milliseconds.apply(1000));
//
//    Set<String> topics = new HashSet<String>();
//    topics.add("spark");
//    Map<String, String> kafkaParams = new HashMap<String, String>();
//    kafkaParams.put("auto.offset.reset", "smallest"); // Or largest
//    kafkaParams.put("bootstrap.servers", "localhost:9092");
//    kafkaParams.put("key.deserializer", "StringDeserializer.class");
//    kafkaParams.put("value.deserializer", "StringDeserializer.class");
//    kafkaParams.put("group.id", jsc.hashCode() + "");
//    kafkaParams.put("enable.auto.commit", "false");
//
//    JavaPairInputDStream<String, String> directKafkaStream =
//      KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class,
//      kafkaParams, topics);
//
//    directKafkaStream.print();
//
//    StructType schema = new StructType(new StructField[] {
//      DataTypes.createStructField("line", DataTypes.StringType, true)
//    });
//
//    jsc.start();
//    jsc.awaitTermination();
//
////
////    Dataset<Row> dataset = spark.readStream().schema(schema).format("csv").load("file:///code/dev/spark-apps/src/main/resources");
////
////    StreamingQuery query = dataset.toDF().writeStream()
////      .format("csv")
////      .option("path", "/tmp/output_data")
////      .option("checkpointLocation", "target/checkpoint")
////      .start();
////
////    query.awaitTermination();
//  }
//}
