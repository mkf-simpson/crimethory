import io.hydrosphere.mist.lib.{MQTTPublisher, MistJob}

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

import twitter4j.Status

object CrimeTwitter extends MistJob with MQTTPublisher {
  override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
    context.setLogLevel("INFO")

    val ssc = new StreamingContext(context, Seconds(30))
    val stream = TwitterUtils.createStream(ssc, None, Array("#trump"))
    stream.foreachRDD { (rdd) =>
      val collected: Array[Status] = rdd
        .filter({ (tweet: Status) => {
          !tweet.getText.startsWith("RT")
        } })
        .collect()
      var idx = 0
      println(s"${collected.length} tweets found")
      while (idx < collected.length) {
        val x = collected(idx)
        publish(Map(
          "text" -> x.getText,
          "screenName" -> x.getUser.getScreenName,
          "name" -> x.getUser.getName,
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
