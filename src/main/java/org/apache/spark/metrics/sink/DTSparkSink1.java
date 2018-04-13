package org.apache.spark.metrics.sink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SecurityManager;

import com.codahale.metrics.MetricRegistry;

/**
 * An implementation of {@link Sink} to capture spark diagnostic information
 */
public class DTSparkSink1 implements Sink
{
  private Properties properties;
  private MetricRegistry registry;
  private SecurityManager securityManager;
  private TestReporter reporter;

  public DTSparkSink1(Properties properties, MetricRegistry registry, SecurityManager securityManager)
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

  /**
   * Report per second by default
   */
  @Override
  public void start()
  {
    reporter.start(1, TimeUnit.SECONDS);
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
