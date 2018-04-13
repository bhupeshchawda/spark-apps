package org.apache.spark.metrics.sink

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import org.apache.spark.SecurityManager
import org.apache.spark.metrics.MetricsSystem

/**
  * Created by bhupesh on 2/4/18.
  */
private[spark] class ScalaSink(val property: Properties, val registry: MetricRegistry,
                             securityMgr: SecurityManager) extends Sink {
  val CSV_KEY_PERIOD = "period"
  val CSV_KEY_UNIT = "unit"
  val CSV_KEY_DIR = "directory"

  val CSV_DEFAULT_PERIOD = 10
  val CSV_DEFAULT_UNIT = "SECONDS"
  val CSV_DEFAULT_DIR = "/tmp/"

  val pollPeriod = Option(property.getProperty(CSV_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CSV_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(CSV_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(CSV_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val pollDir = Option(property.getProperty(CSV_KEY_DIR)) match {
    case Some(s) => s
    case None => CSV_DEFAULT_DIR
  }

  val reporter: PubSubReporter = PubSubReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build()

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
