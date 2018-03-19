package com.datatorrent.spark.io;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by bhupesh on 14/2/18.
 */
public class SparkApoxiUtils
{
  public static JavaStreamingContext getJavaStreamingContext(String sparkMasterUrl, String appName, Duration batchSize)
  {
    JavaStreamingContext jsc = new JavaStreamingContext(sparkMasterUrl, appName, batchSize);
    return jsc;
  }

  public static StreamingContext getStreamingContext(String sparkMasterUrl, String appName, Duration batchSize)
  {
    JavaStreamingContext jsc = new JavaStreamingContext(sparkMasterUrl, appName, batchSize);
    return jsc.ssc();
  }
}
