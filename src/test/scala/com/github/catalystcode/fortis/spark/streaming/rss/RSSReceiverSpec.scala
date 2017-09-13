package com.github.catalystcode.fortis.spark.streaming.rss

import java.net.URL
import java.util
import java.util.Date

import com.rometools.rome.feed.synd.{SyndEntry, SyndFeed}
import org.apache.spark.storage.StorageLevel
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.internal.verification.Times
import org.scalatest.{BeforeAndAfter, FlatSpec}

class RSSReceiverSpec extends FlatSpec with BeforeAndAfter {

  it should "call store for single entry" in {
    val url = new URL("http://bing.com")
    val receiver = new RSSReceiver(Seq(url), StorageLevel.MEMORY_ONLY)
    val receiverSpy = Mockito.spy(receiver)
    val feed = Mockito.mock(classOf[SyndFeed])
    val entry = Mockito.mock(classOf[SyndEntry])
    val publishedDate = new Date

    Mockito.when(entry.getPublishedDate).thenReturn(publishedDate)
    Mockito.when(feed.getEntries).thenReturn(util.Arrays.asList(entry))
    Mockito.doReturn(Seq(Some((url, feed))), null).when(receiverSpy).fetchFeeds()
    Mockito.doNothing().when(receiverSpy).store(any(classOf[RSSEntry]))

    assert(receiverSpy.lastIngestedDates.get(url).isEmpty)
    receiverSpy.poll()
    assert(receiverSpy.lastIngestedDates(url) == publishedDate.getTime)

    Mockito.verify(receiverSpy, new Times(1)).fetchFeeds()
    Mockito.verify(receiverSpy, new Times(1)).store(any(classOf[RSSEntry]))
  }

  it should "store published date of newest entry" in {
    val url = new URL("http://bing.com")
    val receiver = new RSSReceiver(Seq(url), StorageLevel.MEMORY_ONLY)
    val receiverSpy = Mockito.spy(receiver)
    val feed = Mockito.mock(classOf[SyndFeed])

    val entry0 = Mockito.mock(classOf[SyndEntry])
    val publishedDate0 = new Date
    Mockito.when(entry0.getPublishedDate).thenReturn(publishedDate0)

    val entry1 = Mockito.mock(classOf[SyndEntry])
    val publishedDate1 = new Date(publishedDate0.getTime - 100)
    Mockito.when(entry1.getPublishedDate).thenReturn(publishedDate1)

    Mockito.when(feed.getEntries).thenReturn(util.Arrays.asList(
      entry0,
      entry1
    ))
    Mockito.doReturn(Seq(Some((url, feed))), null).when(receiverSpy).fetchFeeds()
    Mockito.doNothing().when(receiverSpy).store(any(classOf[RSSEntry]))

    assert(receiverSpy.lastIngestedDates.get(url).isEmpty)
    receiverSpy.poll()
    assert(receiverSpy.lastIngestedDates(url) == publishedDate0.getTime)

    Mockito.verify(receiverSpy, new Times(1)).fetchFeeds()
    Mockito.verify(receiverSpy, new Times(2)).store(any(classOf[RSSEntry]))
  }

}
