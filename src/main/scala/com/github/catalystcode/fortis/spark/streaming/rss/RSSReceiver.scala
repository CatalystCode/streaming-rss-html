package com.github.catalystcode.fortis.spark.streaming.rss

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

private[rss] class RSSReceiver(feedURLs: Seq[String],
                               requestHeaders: Map[String, String],
                               storageLevel: StorageLevel,
                               connectTimeout: Int = 1000,
                               readTimeout: Int = 1000,
                               pollingPeriodInSeconds: Int = 60)
  extends Receiver[RSSEntry](storageLevel) with Logger {

  @volatile private[rss] var source = new RSSSource(
    feedURLs = feedURLs,
    requestHeaders = requestHeaders,
    connectTimeout = connectTimeout,
    readTimeout = readTimeout
  )

  @volatile private var executor: ScheduledThreadPoolExecutor = _

  def onStart(): Unit = {
    source.reset()
    executor = new ScheduledThreadPoolExecutor(1)

    // Make sure the polling period does not exceed 1 request per second.
    val normalizedPollingPeriod = Math.max(1, pollingPeriodInSeconds)

    executor.scheduleAtFixedRate(new Thread("Polling thread") {
      override def run(): Unit = {
        poll()
      }
    }, 1, normalizedPollingPeriod, TimeUnit.SECONDS)

  }

  def onStop(): Unit = {
    if (executor != null) {
      executor.shutdown()
    }
    source.reset()
  }

  private[rss] def poll(): Unit = {
    try {
      source.fetchEntries().foreach(entry=>{
        store(entry)
      })
    } catch {
      case e: Exception => {
        logError("Unable to fetch RSS entries.", e)
      }
    }
  }

}
