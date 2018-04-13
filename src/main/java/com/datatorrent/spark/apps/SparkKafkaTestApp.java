package com.datatorrent.spark.apps;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

/**
 * Created by bhupesh on 12/2/18.
 */
public class SparkKafkaTestApp
{
  public static Producer producer = null;

  public static void main(String[] args) throws InterruptedException
  {
    JavaStreamingContext jsc = new JavaStreamingContext("spark://earth:7077", "Test", Milliseconds.apply(1000));

    Map<String, Object> kafkaParams = new HashMap<String, Object>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, jsc.hashCode() + "");
    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    Collection<String> topics = Arrays.asList("spark");

    final JavaInputDStream<ConsumerRecord<String, String>> stream =
      KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));


    JavaDStream<Object> stream2 = stream.map(new Function<ConsumerRecord<String,String>, Object>()
    {
      public Object call(ConsumerRecord<String, String> record) throws Exception
      {
        return record.value().toString();
      }
    });


    stream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>()
    {
      public void call(JavaRDD<ConsumerRecord<String, String>> rdd, Time t) throws Exception
      {
        rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>()
        {
          public void call(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws Exception
          {
            while (consumerRecordIterator.hasNext()) {
              getProducer().send(new ProducerRecord("spark-out", consumerRecordIterator.next().value()));
            }
          }
        });
      }
    });

    StructType schema = new StructType(new StructField[] {
      DataTypes.createStructField("line", DataTypes.StringType, true)
    });

    jsc.start();
    jsc.awaitTermination();
    jsc.stop();

  }

  public static Producer getProducer()
  {
    if (producer == null) {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      producer = new KafkaProducer(props);
    }
    return producer;
  }
}
