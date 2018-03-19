package com.datatorrent.spark.io;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

/**
 * Created by bhupesh on 14/2/18.
 */
public class SparkKafka10InputConnector<K, V> implements Serializable
{
  public JavaInputDStream<ConsumerRecord<K, V>> getJavaInputDStream(JavaStreamingContext context,
      Map<String, Object> kafkaParams, Collection<String> topics)
  {
    JavaInputDStream<ConsumerRecord<K, V>> stream =
      KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<K, V>Subscribe(topics, kafkaParams));

    return stream;
  }

  public InputDStream<ConsumerRecord<K, V>> getInputDStream(StreamingContext context,
    Map<String, Object> kafkaParams, Collection<String> topics)
  {
    InputDStream<ConsumerRecord<K, V>> stream =
      KafkaUtils.createDirectStream(context, LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<K, V>Subscribe(topics, kafkaParams));

    return stream;
  }

}
