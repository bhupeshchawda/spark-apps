package org.apache.spark.metrics.sink;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

/**
 * An implementation for {@link Reporter} which collects spark metrics and pushes to required destination
 */
public class TestReporter extends ScheduledReporter
{
  private static final String NAME = "DT-SPARK";
  private static Logger LOG = LoggerFactory.getLogger(TestReporter.class);

  public static Builder forRegistry(MetricRegistry registry)
  {
    return new Builder(registry);
  }

  private TestReporter(MetricRegistry registry,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      MetricFilter filter
  )
  {
    super(registry, NAME, filter, rateUnit, durationUnit);
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers
  )
  {
    System.out.println("Gauges: " + gauges);
    LOG.info("Gauges {}", gauges);
    LOG.info("Counter {}", counters);
    LOG.info("Histogram {}", histograms);
    LOG.info("Meter {}", meters);
    LOG.info("Timer {}", timers);
  }

  /**
   * A builder for {@link TestReporter} instances. Defaults to converting
   * rates to events/second, converting durations to milliseconds, and not filtering metrics.
   */
  public static class Builder
  {
    private final MetricRegistry registry;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;

    private Builder(MetricRegistry registry)
    {
      this.registry = registry;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit)
    {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit)
    {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter)
    {
      this.filter = filter;
      return this;
    }

    /**
     * Builds a {@link TestReporter} with the given properties
     *
     * @return a {@link TestReporter}
     */
    public TestReporter build()
    {
      return new TestReporter(registry,
        rateUnit,
        durationUnit,
        filter);
    }
  }
}
