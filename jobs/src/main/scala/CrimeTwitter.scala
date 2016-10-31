import io.hydrosphere.mist.lib.{MQTTPublisher, MistJob}

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._


object CrimeTwitter extends MistJob with MQTTPublisher {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
    println("crimethory start")
    publish("crimethory start")

    context.setLogLevel("INFO")

    val ssc = new StreamingContext(context, Seconds(30))
    val stream = TwitterUtils.createStream(ssc, None, Array("#crimethory"))
    stream.foreachRDD { (rdd) =>
      val collected = rdd.collect()
      collected.map({(x) => println(x.getText)})
      collected.foreach({(x) => publish(s"[NEW TWEET][[${x.getText}]][[${x.getUser.getScreenName}]][[${x.getUser.getName}]][[${x.getId}]][[${x.getUser.getOriginalProfileImageURL}]]")})
    }

    ssc.start()
    ssc.awaitTermination()

    Map("result" -> "success")
  }
}