import io.hydrosphere.mist.lib.{MQTTPublisher, MistJob}

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._


object CrimeTwitter extends MistJob with MQTTPublisher {

  case class Tweet(text: String, screenName: String, name: String, id: Long, imageURL: String)

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
      val collected = rdd.collect()
      val tweets = collected.map({(x) => Tweet(x.getText, x.getUser.getScreenName, x.getUser.getName, x.getId, x.getUser.getOriginalProfileImageURL)})
      tweets.foreach { (x) =>
//        val str = x.asJson.noSpaces
        println(x)
//        publish(str)
      }
    }

    ssc.start()
    ssc.awaitTermination()

    Map("result" -> "success")
  }
}
