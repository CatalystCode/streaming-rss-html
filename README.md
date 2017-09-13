[![Travis CI status](https://api.travis-ci.org/CatalystCode/streaming-reddit.svg?branch=master)](https://travis-ci.org/CatalystCode/streaming-reddit)

# streaming-rss-html

A library for reading public RSS feeds using Spark Streaming.

## Usage example ##

Run a demo via:

```sh
# compile scala, run tests, build fat jar
sbt assembly

# run on spark
spark-submit --class RSSDemo target/scala-2.11/streaming-rss-html-assembly-0.0.1.jar http://somehost/somepath/to/rss
```

Add to your own project by adding this dependency in your `build.sbt`:

```
libraryDependencies ++= Seq(
  //...
  "com.github.catalystcode" %% "streaming-rss-html" % "0.0.1",
  //...
)
```

## How does it work? ##

Currently, this RDDInputDStream polls the given RSS feed at the specified rated. All scraping of any HTML content is up
to the caller.

## Release process ##

1. Configure your credentials via the `SONATYPE_USER` and `SONATYPE_PASSWORD` environment variables.
2. Update `version.sbt`
3. Run `sbt` then from the sbt shell, do this:

```
sonatypeOpen "enter staging description here"
sbt publishSigned
sbt sonatypeRelease
```

