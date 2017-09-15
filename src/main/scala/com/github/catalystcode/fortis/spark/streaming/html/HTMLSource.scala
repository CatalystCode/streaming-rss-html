package com.github.catalystcode.fortis.spark.streaming.html

import java.net.URL
import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import org.apache.spark.unsafe.types.UTF8String
import org.jsoup.nodes.Document
import org.jsoup.{Connection, Jsoup}

import scala.collection.JavaConversions._

private[html] class HTMLSource(siteURL: URL,
                               maxDepth: Int = 1,
                               requestHeaders: Map[String, String] = Map(),
                               cacheEditDistanceThreshold: Double = 0.10) extends Serializable with Logger {

  private val connectTimeoutMillis: Int = sys.env.getOrElse("HTML_SOURCE_CONNECT_TIMEOUT_MILLIS", "500").toInt
  private val cacheTimeMinutes: Int = sys.env.getOrElse("HTML_SOURCE_CACHE_TIME_MINUTES", "30").toInt

  private val cache = CacheBuilder.newBuilder().expireAfterWrite(cacheTimeMinutes, TimeUnit.MINUTES).build[URL, Document]()
  private[html] var connector = new HTMLConnector()

  def reset(): Unit = {
    cache.cleanUp()
  }

  private[html] class HTMLConnector extends Serializable {
    def connect(url: URL): Connection = {
      Jsoup.connect(url.toString).timeout(connectTimeoutMillis).headers(requestHeaders).followRedirects(true)
    }
  }

  def fetch(): Seq[HTMLPage] = {
    val documentPairs = unfilteredDocuments()
    documentPairs
      .filter(pair=>{
        val url = pair._1
        val document = pair._2
        val cachedDocument = cache.getIfPresent(url)
        if (cachedDocument == null) {
          cache.put(url, document)
          true
        } else {
          val documentText = document.body().text() match {
            case null => UTF8String.EMPTY_UTF8
            case str => UTF8String.fromString(str)
          }
          val cachedDocumentText = cachedDocument.body().text() match {
            case null => UTF8String.EMPTY_UTF8
            case str => UTF8String.fromString(str)
          }
          val distance = documentText.levenshteinDistance(cachedDocumentText)
          val totalCharCount = documentText.numChars() + cachedDocumentText.numChars()
          val distanceAsPercentageOfTotalCount = distance / totalCharCount.toDouble
          distanceAsPercentageOfTotalCount > cacheEditDistanceThreshold
        }
      })
      .map(p => HTMLPage(p._1.toString, p._2.html()))
  }

  private val urlPattern = raw"http[s]?://.+".r
  private val rootPathPattern = raw"[/]+".r
  private val absolutePathPattern = raw"[/].+".r
  private val blankPattern = raw"\\s+".r

  private[html] def fetchDocument(url: URL): Option[Document] = {
    try {
      val connection = connector.connect(siteURL)
      Some(connection.get())
    } catch {
      case e: Exception => {
        logError(s"Unable to fetch document for $siteURL", e)
        None
      }
    }
  }

  private[html] def unfilteredDocuments(): Seq[(URL,Document)] = {
    val rootHost = siteURL.getHost
    val rootPortString = siteURL.getPort match {
      case -1 => ""
      case _ => s":${siteURL.getPort}"
    }

    fetchDocument(siteURL) match {
      case None => Seq()
      case Some(rootDocument) => {
        if (maxDepth < 1) {
          return Seq((siteURL, rootDocument))
        }

        val anchors = rootDocument.select("a[href]")
        val childURLs = anchors match {
          case null => Seq()
          case _ => anchors
            .iterator()
            .toSeq
            .filter(a=>a.hasText && a.hasAttr("href"))
            .par
            .map(a=>{
              val href = a.attr("href")
              try {
                val url = href match {
                  case urlPattern() => new URL(href)
                  case rootPathPattern() => siteURL
                  case blankPattern() => siteURL
                  case absolutePathPattern() => new URL(s"${siteURL.getProtocol}://$rootHost$rootPortString$href")
                  case _ => new URL(s"$siteURL/$href")
                }
                Some(url)
              } catch {
                case e: Exception => None
              }
            })
            .filter(d=>d.isDefined)
            .map(d=>d.get)
        }
        val childDocuments = childURLs.map(childURL => {
          if (childURL == siteURL) {
            None
          } else if (childURL.getHost != rootHost) {
            None
          }
          else {
            fetchDocument(childURL) match {
              case None => None
              case Some(childDocument) => Some[(URL, Document)](childURL, childDocument)
            }
          }
        }).filter(d=>d.isDefined).map(d=>d.get)
        Seq((siteURL, rootDocument)) ++ childDocuments
      }
    }
  }

}
