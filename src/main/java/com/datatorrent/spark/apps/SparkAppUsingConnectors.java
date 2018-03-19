package com.datatorrent.spark.apps;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datatorrent.spark.io.SparkKafka10InputConnector;
import com.datatorrent.spark.io.SparkKafka10OutputConnector;

/**
 * Created by bhupesh on 16/2/18.
 */
public class SparkAppUsingConnectors
{
  public static void main(String[] args) throws InterruptedException
  {
    JavaStreamingContext jsc = new JavaStreamingContext("spark://earth:7077", "Test", Milliseconds.apply(1000));

    // Read using input connector
    Map<String, Object> kafkaParams = new HashMap<String, Object>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, jsc.hashCode() + "");
    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    Collection<String> topics = Arrays.asList("spark");

    JavaInputDStream input = new SparkKafka10InputConnector<String, String>().getJavaInputDStream(jsc, kafkaParams, topics);

    // Publish using output connector
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    SparkKafka10OutputConnector<String, String> outputConnector = new SparkKafka10OutputConnector<String, String>(props);
    outputConnector.setProducerRecordGetter(new SparkKafka10OutputConnector.ProducerRecordGetter<String, String>()
    {
      public ProducerRecord<String, String> get(Object o)
      {
        String value = (String)((ConsumerRecord)o).value();
        return new ProducerRecord<String, String>("spark-out", value);
      }
    });
    outputConnector.publish(input);

    jsc.start();
    jsc.awaitTermination();
  }
}
