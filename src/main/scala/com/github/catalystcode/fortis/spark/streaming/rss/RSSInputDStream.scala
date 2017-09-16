package com.github.catalystcode.fortis.spark.streaming.rss

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

class RSSInputDStream(feedURLs: Seq[String],
                      requestHeaders: Map[String, String],
                      ssc: StreamingContext,
                      storageLevel: StorageLevel,
                      pollingPeriodInSeconds: Int = 60)
  extends ReceiverInputDStream[RSSEntry](ssc) {

  override def getReceiver(): Receiver[RSSEntry] = {
    logDebug("Creating RSS receiver")
    new RSSReceiver(feedURLs, requestHeaders, storageLevel, pollingPeriodInSeconds)
  }

}
