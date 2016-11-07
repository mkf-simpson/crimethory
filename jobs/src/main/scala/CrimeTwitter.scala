import io.hydrosphere.mist.lib.{MQTTPublisher, MistJob}

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

import twitter4j.Status

object CrimeTwitter extends MistJob with MQTTPublisher {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
    context.setLogLevel("INFO")

    val ssc = new StreamingContext(context, Seconds(30))
    val stream = TwitterUtils.createStream(ssc, None, Array("#crimethory"))
    stream.foreachRDD { (rdd) =>
      val collected: Array[Status] = rdd.collect()
      var idx = 0
      while (idx < collected.length) {
        val x = collected(idx)
        publish(Map(
          "text" -> x.getText,
          "screenName" -> s.getUser.getScreenName,
          "name" -> s.getUser.getName,
          "id" -> x.getId
        ))
        idx += 1
      }
    }

    ssc.start()
    ssc.awaitTermination()

    Map("result" -> "success")
  }
}
