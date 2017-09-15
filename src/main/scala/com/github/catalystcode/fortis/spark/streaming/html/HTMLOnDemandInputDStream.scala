package com.github.catalystcode.fortis.spark.streaming.html

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}

class HTMLOnDemandInputDStream(siteURLs: Seq[URL],
                               ssc: StreamingContext,
                               maxDepth: Int = 1,
                               requestHeaders: Map[String, String] = Map(),
                               cacheEditDistanceThreshold: Double = 0.10) extends InputDStream[HTMLPage](ssc) {

  private var sources: Seq[HTMLSource] = Seq()

  override def start(): Unit = {
    sources = siteURLs.map(url => { new HTMLSource(
        url,
        maxDepth = maxDepth,
        requestHeaders = requestHeaders,
        cacheEditDistanceThreshold = cacheEditDistanceThreshold
    )})
  }

  override def stop(): Unit = {
    sources = Seq()
  }

  override def compute(validTime: Time): Option[RDD[HTMLPage]] = {
    Some(this.context.sparkContext.parallelize(
      sources.flatMap(_.fetch())
    ))
  }
}
