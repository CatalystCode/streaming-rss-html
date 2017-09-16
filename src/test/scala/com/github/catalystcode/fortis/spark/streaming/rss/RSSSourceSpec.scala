package com.github.catalystcode.fortis.spark.streaming.rss

import java.util
import java.util.Date

import com.rometools.rome.feed.synd.{SyndEntry, SyndFeed}
import org.mockito.Mockito
import org.mockito.internal.verification.Times
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.mutable

class RSSSourceSpec extends FlatSpec with BeforeAndAfter {

  it should "reset ingest dates to MinValue" in {
    val url = "http://bing.com"
    val source = new RSSSource(Seq(url), Map[String, String]())

    assert(source.lastIngestedDates == mutable.Map[String, Long]())
    source.reset()
    assert(source.lastIngestedDates == mutable.Map[String, Long](
      url -> Long.MinValue
    ))
  }

  it should "return a single entry for rss feed" in {
    val url = "http://bing.com"
    val source = new RSSSource(Seq(url), Map[String, String]())
    val sourceSpy = Mockito.spy(source)

    val feed = Mockito.mock(classOf[SyndFeed])
    val entry = Mockito.mock(classOf[SyndEntry])
    val publishedDate = new Date

    Mockito.when(entry.getPublishedDate).thenReturn(publishedDate)
    Mockito.when(feed.getEntries).thenReturn(util.Arrays.asList(entry))
    Mockito.doReturn(Seq(Some((url, feed))), null).when(sourceSpy).fetchFeeds()

    assert(source.lastIngestedDates.get(url).isEmpty)
    val entries0 = sourceSpy.fetchEntries()
    assert(source.lastIngestedDates(url) == publishedDate.getTime)
    assert(entries0 == Seq(
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
    Mockito.verify(sourceSpy, new Times(1)).fetchFeeds()

    Mockito.when(feed.getEntries).thenReturn(util.Arrays.asList(entry))
    Mockito.doReturn(Seq(Some((url, feed))), null).when(sourceSpy).fetchFeeds()
    val entries1 = sourceSpy.fetchEntries()
    assert(entries1 == Seq())
    Mockito.verify(sourceSpy, new Times(2)).fetchFeeds()
  }

  it should "store published date of newest entry" in {
    val url = "http://bing.com"
    val source = new RSSSource(Seq(url), Map[String, String]())
    val sourceSpy = Mockito.spy(source)

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
    Mockito.doReturn(Seq(Some((url, feed))), null).when(sourceSpy).fetchFeeds()

    assert(source.lastIngestedDates.get(url).isEmpty)
    val entries = sourceSpy.fetchEntries()
    assert(source.lastIngestedDates(url) == publishedDate0.getTime)
    assert(entries == Seq(
      RSSEntry(
        RSSFeed(null,null,null,null,null),
        null,
        null,
        List(),
        List(),
        null,
        List(),
        entry0.getPublishedDate.getTime,
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
        entry1.getPublishedDate.getTime,
        0,
        List(),
        List()
      )
    ))
    Mockito.verify(sourceSpy, new Times(1)).fetchFeeds()
  }

}
