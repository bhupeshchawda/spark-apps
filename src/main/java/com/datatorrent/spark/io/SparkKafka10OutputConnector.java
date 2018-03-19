package com.datatorrent.spark.io;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Created by bhupesh on 14/2/18.
 */
public class SparkKafka10OutputConnector<K, V> implements Serializable
{
  private Properties props;
  private ProducerRecordGetter<K, V> producerRecordGetter;

  public SparkKafka10OutputConnector(Properties props)
  {
    this.props = props;
  }

  public void publish(JavaDStream<Object> stream)
  {
    stream.foreachRDD(new VoidFunction<JavaRDD<Object>>()
    {
      public void call(JavaRDD<Object> objectJavaRDD) throws Exception
      {
        objectJavaRDD.foreachPartition(new VoidFunction<Iterator<Object>>()
        {
          public void call(Iterator<Object> objectIterator) throws Exception
          {
            Producer producer = getProducer();
            while (objectIterator.hasNext()) {
              Object object = objectIterator.next();
              producer.send(getProducerRecordGetter().get(object));
            }
          }
        });
      }
    });
  }

  public KafkaProducer<K, V> getProducer()
  {
    return new KafkaProducer<K, V>(props);
  }

  public interface ProducerRecordGetter<K, V> extends Serializable
  {
    ProducerRecord<K, V> get(Object o);
  }

  public ProducerRecordGetter<K, V> getProducerRecordGetter()
  {
    return producerRecordGetter;
  }

  public void setProducerRecordGetter(ProducerRecordGetter<K, V> producerRecordGetter)
  {
    this.producerRecordGetter = producerRecordGetter;
  }
}
