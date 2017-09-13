package com.github.catalystcode.fortis.spark.streaming.rss

import java.net.URL
import java.util.Date
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.rometools.rome.feed.synd.SyndFeed
import com.rometools.rome.io.{SyndFeedInput, XmlReader}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.JavaConversions._

private[rss] class RSSReceiver(feedURL: URL, storageLevel: StorageLevel, pollingPeriodInSeconds: Int = 3)
  extends Receiver[RSSEntry](storageLevel) with Logger {

  @volatile private[rss] var lastIngestedDate = Long.MinValue

  private var executor: ScheduledThreadPoolExecutor = _

  def onStart(): Unit = {
    val threadCount = 1
    executor = new ScheduledThreadPoolExecutor(threadCount)

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
  }

  private[rss] def fetchFeed(): SyndFeed = {
    val reader = new XmlReader(feedURL)
    new SyndFeedInput().build(reader)
  }

  private[rss] def poll(): Unit = {
    fetchFeed().getEntries
      .filter(entry=>{
        val date = Math.max(safeDateGetTime(entry.getPublishedDate), safeDateGetTime(entry.getUpdatedDate))
        logDebug(s"Received RSS entry ${entry.getUri} from date ${date}")
        date > lastIngestedDate
      })
      .map(entry=>new RSSEntry(
          uri = entry.getUri,
          title = entry.getTitle,
          links = entry.getLinks.map(l=>new RSSLink(href = l.getHref, title = l.getTitle)).toList,
          content = entry.getContents.map(c=> new RSSContent(contentType = c.getType, mode = c.getMode, value = c.getValue)).toList,
          description = entry.getDescription match {
            case null => null
            case d => new RSSContent(
              contentType = d.getType,
              mode = d.getMode,
              value = d.getValue
            )
          },
          enclosures = entry.getEnclosures.map(e=>new RSSEnclosure(url = e.getUrl, enclosureType = e.getType, length = e.getLength)).toList,
          publishedDate = safeDateGetTime(entry.getPublishedDate),
          updatedDate = safeDateGetTime(entry.getUpdatedDate),
          authors = entry.getAuthors.map(a=>new RSSPerson(name = a.getName, uri = a.getUri, email = a.getEmail)).toList,
          contributors = entry.getContributors.map(c=>new RSSPerson(name = c.getName, uri = c.getUri, email = c.getEmail)).toList
        ))
      .foreach(entry=>{
        store(entry)
        markStored(entry)
      })
  }

  private def markStored(entry: RSSEntry): Unit = {
    val date = entry.updatedDate match {
      case 0 => entry.publishedDate
      case _ => entry.updatedDate
    }
    if (date > lastIngestedDate) {
      lastIngestedDate = date
    }
    logDebug(s"Updating last ingested date to ${date}")
  }

  private def safeDateGetTime(date: Date): Long = {
    Option(date).map(_.getTime).getOrElse(0)
  }

}
