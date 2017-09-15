import java.net.URL

import com.github.catalystcode.fortis.spark.streaming.html.HTMLInputDStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object HTMLDemo {
  def main(args: Array[String]) {
    val durationSeconds = 10
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))

    val urlCSV = args(0)
    val urls = urlCSV.split(",").map(new URL(_))
    val stream = new HTMLInputDStream(urls, ssc)
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
