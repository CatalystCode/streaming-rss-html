import com.github.catalystcode.fortis.spark.streaming.rss.RSSInputDStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RSSDemo {
  def main(args: Array[String]) {
    val durationSeconds = 10
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")

    val urlCSV = args(0)
    val urls = urlCSV.split(",")
    val stream = new RSSInputDStream(urls, Map[String, String](), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)
    stream.foreachRDD(rdd=>{
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      rdd.toDS().show()
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
}
