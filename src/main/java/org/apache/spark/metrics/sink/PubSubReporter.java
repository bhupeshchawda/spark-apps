package org.apache.spark.metrics.sink;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.jetty.websocket.WebSocketClient;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import com.datatorrent.stram.util.SharedPubSubWebSocketClient;

public class PubSubReporter extends ScheduledReporter
{
  private WebSocketClient wsClient;

  public static PubSubReporter.Builder forRegistry(MetricRegistry registry) {
    return new PubSubReporter.Builder(registry);
  }

  private PubSubReporter(MetricRegistry registry,
    TimeUnit rateUnit,
    TimeUnit durationUnit,
    MetricFilter filter) {
    super(registry, "csv-reporter", filter, rateUnit, durationUnit);
  }

  public void setup()
  {
    SharedPubSubWebSocketClient wsClient = null;
    boolean gatewayUseSsl = false;
    try {
      wsClient = new SharedPubSubWebSocketClient((gatewayUseSsl ? "wss://" : "ws://") + "localhost:9090" + "/pubsub", 5000);
      wsClient.openConnection();
    } catch (URISyntaxException | InterruptedException | ExecutionException | TimeoutException | IOException e) {
      throw new RuntimeException("Exception in creating web socket client");
    }
  }

  @Override
  public void report(SortedMap<String, Gauge> sortedMap, SortedMap<String, Counter> sortedMap1, SortedMap<String, Histogram> sortedMap2, SortedMap<String, Meter> sortedMap3, SortedMap<String, Timer> sortedMap4)
  {
    System.out.println("Gauges: " + sortedMap);
    System.out.println("Counter: " + sortedMap1);
    System.out.println("Histogram: " + sortedMap2);
    System.out.println("Meter: " + sortedMap3);
    System.out.println("Timer: " + sortedMap4);
  }

  /**
   * A builder for {@link CsvReporter} instances. Defaults to using the default locale, converting
   * rates to events/second, converting durations to milliseconds, and not filtering metrics.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;

    private Builder(MetricRegistry registry) {
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
    public PubSubReporter.Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public PubSubReporter.Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public PubSubReporter.Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Builds a {@link CsvReporter} with the given properties, writing {@code .csv} files to the
     * given directory.
     *
     * @return a {@link CsvReporter}
     */
    public PubSubReporter build() {
      return new PubSubReporter(registry,
        rateUnit,
        durationUnit,
        filter);
    }
  }

}
