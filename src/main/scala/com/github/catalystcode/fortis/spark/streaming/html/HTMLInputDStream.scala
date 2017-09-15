package com.github.catalystcode.fortis.spark.streaming.html

import java.net.URL

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

class HTMLInputDStream(siteURLs: Seq[URL],
                       ssc: StreamingContext,
                       storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                       maxDepth: Int = 1,
                       requestHeaders: Map[String, String] = Map(),
                       pollingPeriodInSeconds: Int = 60,
                       cacheEditDistanceThreshold: Double = 0.10) extends ReceiverInputDStream[HTMLPage](ssc) {
  override def getReceiver(): Receiver[HTMLPage] = {
    new HTMLReceiver(
      siteURLs = siteURLs,
      storageLevel = storageLevel,
      maxDepth = maxDepth,
      requestHeaders = requestHeaders,
      pollingPeriodInSeconds = pollingPeriodInSeconds,
      cacheEditDistanceThreshold = cacheEditDistanceThreshold
    )
  }

}
