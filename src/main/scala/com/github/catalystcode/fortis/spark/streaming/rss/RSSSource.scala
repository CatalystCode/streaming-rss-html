package com.github.catalystcode.fortis.spark.streaming.rss

import java.net.URL
import java.util.Date

import com.rometools.rome.feed.synd.{SyndEntry, SyndFeed}
import com.rometools.rome.io.{SyndFeedInput, XmlReader}

import scala.collection.JavaConversions._
import scala.collection.mutable

private[rss] class RSSSource(feedURLs: Seq[String], requestHeaders: Map[String, String], connectTimeout: Int = 1000, readTimeout: Int = 1000) extends Serializable with Logger {

  private[rss] var lastIngestedDates = mutable.Map[String, Long]()

  def reset(): Unit = {
    lastIngestedDates.clear()
    feedURLs.foreach(url=>{
      lastIngestedDates.put(url, Long.MinValue)
    })
  }

  def getLinks(feedEntry: SyndEntry): scala.List[RSSLink] = {
    val link = feedEntry.getLink
    val transformedLink = if (link == null) List() else List(RSSLink(href = link, title = ""))

    val links = feedEntry.getLinks
    val transformedLinks =  if (links == null) List() else links.map(l => RSSLink(href = l.getHref, title = l.getTitle)).toList

    transformedLink ++ transformedLinks
  }

  def fetchEntries(): Seq[RSSEntry] = {
    fetchFeeds()
      .filter(_.isDefined)
      .flatMap(optionPair=>{
        val url = optionPair.get._1
        val feed = optionPair.get._2

        val source = RSSFeed(
          feedType = feed.getFeedType,
          uri = url,
          title = feed.getTitle,
          description = feed.getDescription,
          link = feed.getLink
        )

        feed.getEntries
          .filter(entry=>{
            val date = Math.max(safeDateGetTime(entry.getPublishedDate), safeDateGetTime(entry.getUpdatedDate))
            lastIngestedDates.get(url).isEmpty || date > lastIngestedDates(url)
          })
          .map(feedEntry=>{
            val entry = RSSEntry(
              source = source,
              uri = feedEntry.getUri,
              title = feedEntry.getTitle,
              links = getLinks(feedEntry),
              content = feedEntry.getContents.map(c => RSSContent(contentType = c.getType, mode = c.getMode, value = c.getValue)).toList,
              description = feedEntry.getDescription match {
                case null => null
                case d => RSSContent(
                  contentType = d.getType,
                  mode = d.getMode,
                  value = d.getValue
                )
              },
              enclosures = feedEntry.getEnclosures.map(e => RSSEnclosure(url = e.getUrl, enclosureType = e.getType, length = e.getLength)).toList,
              publishedDate = safeDateGetTime(feedEntry.getPublishedDate),
              updatedDate = safeDateGetTime(feedEntry.getUpdatedDate),
              authors = feedEntry.getAuthors.map(a => RSSPerson(name = a.getName, uri = a.getUri, email = a.getEmail)).toList,
              contributors = feedEntry.getContributors.map(c => RSSPerson(name = c.getName, uri = c.getUri, email = c.getEmail)).toList
            )
            markStored(entry, url)
            entry
          })
      })
  }

  private[rss] def fetchFeeds(): Seq[Option[(String, SyndFeed)]] = {
    feedURLs.map(url=>{
      try {
        val connection = new URL(url).openConnection()
        connection.setConnectTimeout(connectTimeout)
        connection.setReadTimeout(readTimeout)
        val reader = new XmlReader(connection, requestHeaders)
        val feed = new SyndFeedInput().build(reader)
        Some((url, feed))
      } catch {
        case e: Exception => {
          logError(s"Unable to fetch ${url}", e)
          None
        }
      }
    })
  }

  private def markStored(entry: RSSEntry, url: String): Unit = {
    val date = entry.updatedDate match {
      case 0 => entry.publishedDate
      case _ => entry.updatedDate
    }
    lastIngestedDates.get(url) match {
      case Some(lastIngestedDate) => if (date > lastIngestedDate) {
          lastIngestedDates.put(url, date)
      }
      case None => lastIngestedDates.put(url, date)
    }
  }

  private def safeDateGetTime(date: Date): Long = {
    Option(date).getOrElse(new Date()).getTime
  }

}
