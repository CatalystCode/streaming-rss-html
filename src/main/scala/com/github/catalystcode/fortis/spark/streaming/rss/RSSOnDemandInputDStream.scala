package com.github.catalystcode.fortis.spark.streaming.rss

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}

class RSSOnDemandInputDStream(feedURLs: Seq[URL],
                              requestHeaders: Map[String, String],
                              ssc: StreamingContext) extends InputDStream[RSSEntry](ssc) {

  @volatile private[rss] var source = new RSSSource(feedURLs, requestHeaders)

  override def start(): Unit = {
    source.reset()
    println(s"Started ${this}")
  }

  override def stop(): Unit = {
    source.reset()
    println(s"Stopped ${this}")
  }

  override def compute(validTime: Time): Option[RDD[RSSEntry]] = {
    Some(this.context.sparkContext.parallelize(
      source.fetchEntries()
    ))
  }

}
