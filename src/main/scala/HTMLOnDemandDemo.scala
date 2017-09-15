import java.net.URL

import com.github.catalystcode.fortis.spark.streaming.html.{HTMLInputDStream, HTMLOnDemandInputDStream}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object HTMLOnDemandDemo {
  def main(args: Array[String]) {
    val durationSeconds = 10
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))

    val urlCSV = args(0)
    val urls = urlCSV.split(",").map(new URL(_))
    val stream = new HTMLOnDemandInputDStream(
      urls,
      ssc,
      requestHeaders = Map[String, String](
        "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
      )
    )
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
