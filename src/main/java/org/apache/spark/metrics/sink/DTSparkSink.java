package org.apache.spark.metrics.sink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SecurityManager;
import org.apache.spark.metrics.sink.Sink;

import com.codahale.metrics.MetricRegistry;

/**
 * Created by bhupesh on 2/4/18.
 */
public class DTSparkSink implements Sink
{
  private Properties properties;
  private MetricRegistry registry;
  private SecurityManager securityManager;
  private TestReporter reporter;

  public DTSparkSink(Properties properties, MetricRegistry registry, SecurityManager securityManager)
  {
    this.properties = properties;
    this.registry = registry;
    this.securityManager = securityManager;

    System.out.println("Spark Metric Sink created");

    reporter = TestReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.SECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build();
  }

  @Override
  public void start()
  {
    reporter.start(5, TimeUnit.SECONDS);
  }

  @Override
  public void stop()
  {
    reporter.stop();
  }

  @Override
  public void report()
  {
    reporter.report();
  }
}
