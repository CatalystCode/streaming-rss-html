package com.github.catalystcode.fortis.spark.streaming.rss

import java.net.URL
import java.util.Date

import org.apache.spark.storage.StorageLevel
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.internal.verification.Times
import org.scalatest.{BeforeAndAfter, FlatSpec}

class RSSReceiverSpec extends FlatSpec with BeforeAndAfter {

  it should "call store for each entry" in {
    val url = new URL("http://bing.com")
    val source = Mockito.mock(classOf[RSSSource])
    val receiver = new RSSReceiver(Seq(url), null, StorageLevel.MEMORY_ONLY)
    receiver.source = source

    val receiverSpy = Mockito.spy(receiver)
    Mockito.doNothing().when(receiverSpy).store(any(classOf[RSSEntry]))

    val publishedDate = new Date
    Mockito.when(source.fetchEntries()).thenReturn(Seq(
      RSSEntry(
        RSSFeed(null,null,null,null,null),
        null,
        null,
        List(),
        List(),
        null,
        List(),
        publishedDate.getTime,
        0,
        List(),
        List()
      ),
      RSSEntry(
        RSSFeed(null,null,null,null,null),
        null,
        null,
        List(),
        List(),
        null,
        List(),
        publishedDate.getTime,
        0,
        List(),
        List()
      ),
      RSSEntry(
        RSSFeed(null,null,null,null,null),
        null,
        null,
        List(),
        List(),
        null,
        List(),
        publishedDate.getTime,
        0,
        List(),
        List()
      )
    ))

    receiverSpy.poll()

    Mockito.verify(source, new Times(1)).fetchEntries()
    Mockito.verify(receiverSpy, new Times(3)).store(any(classOf[RSSEntry]))
  }

}
