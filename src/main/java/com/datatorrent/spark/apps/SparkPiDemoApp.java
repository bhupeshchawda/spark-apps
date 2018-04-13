package com.datatorrent.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by bhupesh on 19/3/18.
 */
public class SparkPiDemoApp
{
  public static void main(String[] args) throws InterruptedException
  {
    SparkConf conf = new SparkConf();
    JavaStreamingContext jsc = new JavaStreamingContext(conf, Milliseconds.apply(1000));

    jsc.textFileStream("/tmp/sparkin").map(new Function<String, String[]>()
    {
      public String[] call(String v1) throws Exception
      {
        return v1.split(" ");
      }
    }).count().dstream().print();

    jsc.start();
    jsc.awaitTermination();
  }
}
