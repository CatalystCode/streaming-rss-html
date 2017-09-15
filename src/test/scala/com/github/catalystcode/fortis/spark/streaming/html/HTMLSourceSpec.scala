package com.github.catalystcode.fortis.spark.streaming.html

import java.net.URL

import org.jsoup.Connection
import org.jsoup.nodes.{Document, Element}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec}

class HTMLSourceSpec extends FlatSpec with BeforeAndAfter with MockitoSugar {

  it should "return a single entry for page without any links" in {
    val url = new URL("http://bing.com")
    val source = new HTMLSource(url)
    source.connector = mock[source.HTMLConnector]

    val body = mock[Element]
    val text = "These are the contents of the page"
    Mockito.when(body.text()).thenReturn(text)

    val document = mock[Document]
    Mockito.when(document.body()).thenReturn(body)

    val html = s"<h1>$text</h1>"
    Mockito.when(document.html()).thenReturn(html)

    val connection = mock[Connection]
    Mockito.when(connection.get()).thenReturn(document)

    Mockito.when(source.connector.connect(ArgumentMatchers.any())).thenReturn(connection)

    val documents = source.fetch()
    assert(documents == Seq(HTMLPage(url.toString, html)))
  }

}
