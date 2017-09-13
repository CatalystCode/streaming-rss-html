package com.github.catalystcode.fortis.spark.streaming.rss

case class RSSEntry(
  source: RSSFeed,
  uri: String,
  title: String,
  links: List[RSSLink],
  content: List[RSSContent],
  description: RSSContent,
  enclosures: List[RSSEnclosure],
  publishedDate: Long,
  updatedDate: Long,
  authors: List[RSSPerson],
  contributors: List[RSSPerson]
)

case class RSSFeed(feedType: String, uri: String, title: String, description: String, link: String)
case class RSSLink(href: String, title: String)
case class RSSContent(contentType: String, mode: String, value: String)
case class RSSEnclosure(url: String, enclosureType: String, length: Long)
case class RSSPerson(name: String, uri: String, email: String)
case class RSSCategory(name: String, taxonomyUri: String)
