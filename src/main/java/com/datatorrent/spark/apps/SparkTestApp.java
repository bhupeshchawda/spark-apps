package com.datatorrent.spark.apps;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datatorrent.library.ledger.api.serde.SerDe;
import com.datatorrent.library.ledger.api.serde.SerDeFactory;

public class SparkTestApp {
  private static final String HOST = "localhost";
  private static final int PORT = 9999;

  public static void main(String[] args) throws InterruptedException, IOException
  {
    final EventServer server = new EventServer();

    Executors.newSingleThreadExecutor().execute(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          server.main(new String[]{});
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    // Configure and initialize the SparkStreamingContext
    SparkConf conf = new SparkConf();
    System.out.println("Configuration: " + conf.toDebugString());
    JavaStreamingContext streamingContext =
      new JavaStreamingContext(conf, Durations.seconds(5));

    System.out.println("New Configuration: " + streamingContext.sparkContext().sc().getConf().toDebugString());
    // Receive streaming data from the source
    JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(HOST, PORT);
    lines.print();

    // Execute the Spark workflow defined above
    streamingContext.start();
    Thread.sleep(10000);
    System.out.println("Reporting Now");
    System.out.println("Metrics System" + streamingContext.sparkContext().env().metricsSystem().toString());
    streamingContext.awaitTermination();
  }
}