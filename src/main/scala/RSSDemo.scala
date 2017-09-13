import java.net.URL

import com.github.catalystcode.fortis.spark.streaming.rss.RSSInputDStream
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RSSDemo {
  def main(args: Array[String]) {
    BasicConfigurator.configure()
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("lib-rss").setLevel(Level.DEBUG)

    // set up the spark context and streams
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val durationSeconds = 10
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))

    val url = new URL(args(0))
    val stream = new RSSInputDStream(url, ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = 10)
    stream.foreachRDD(rdd=>{
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      rdd.toDS().show(100)
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
}
