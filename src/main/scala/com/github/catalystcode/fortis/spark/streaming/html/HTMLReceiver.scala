package com.github.catalystcode.fortis.spark.streaming.html

import java.net.URL
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class HTMLReceiver(siteURLs: Seq[URL],
                   storageLevel: StorageLevel,
                   maxDepth: Int = 1,
                   pollingPeriodInSeconds: Int = 60,
                   requestHeaders: Map[String, String] = Map(),
                   cacheEditDistanceThreshold: Double = 0.10)
  extends Receiver[HTMLPage](storageLevel) with Logger {

  @volatile private var sources: Seq[HTMLSource] = Seq()
  @volatile private var executor: ScheduledThreadPoolExecutor = _

  override def onStart(): Unit = {
    sources = siteURLs.map(url => new HTMLSource(url))
    executor = new ScheduledThreadPoolExecutor(1)

    // Make sure the polling period does not exceed 1 request per second.
    val normalizedPollingPeriod = Math.max(1, pollingPeriodInSeconds)

    executor.scheduleAtFixedRate(new Thread("Polling thread") {
      override def run(): Unit = {
        poll()
      }
    }, 1, normalizedPollingPeriod, TimeUnit.SECONDS)

  }

  override def onStop(): Unit = {
    if (executor != null) {
      executor.shutdown()
    }
    sources = Seq()
  }

  private[html] def poll(): Unit = {
    sources.flatMap(s => s.fetch())
      .foreach(page => store(page))
  }

}
