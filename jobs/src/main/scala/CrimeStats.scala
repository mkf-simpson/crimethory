
import io.hydrosphere.mist.lib.{MistJob, SQLSupport}

import scala.math.BigInt
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SQLContext
import java.nio.file.Paths
import java.nio.file.Files

import org.apache.spark.ml.PipelineModel

object CrimeStats extends MistJob with SQLSupport {

  def predict(month: Int, x: Int, lat: Double, lon: Double, contextSQL: SQLContext, model: PipelineModel): Double = {
    if (Files.exists(Paths.get(s"model_${lat}_${lon}"))) {
      val featureReq = Seq((1.0, Vectors.dense(month.toDouble / 12.0)))
      val requestData = contextSQL.createDataFrame(featureReq).toDF("label", "features")

      val prediction = model.transform(requestData)
      prediction.show()
      val predictedCrime = prediction.select("prediction").head()
      if (predictedCrime.size > 0)
        println("crime: ", predictedCrime.toString)
      predictedCrime(0).toString.toDouble / 100 + scala.math.abs(scala.math.sin((lat + lon + month) * x))
    }
    else {
      0.0
    }
  }

  def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

    val contextSQL = session.sqlContext
    val context = session.sparkContext

    val lat = parameters("lat").asInstanceOf[String].toDouble
    val lng = parameters("lng").asInstanceOf[String].toDouble
    val month = parameters("month").asInstanceOf[BigInt].intValue

    val model = PipelineModel.load(s"model_${lat}_${lng}")

    val byPointK = Array(2, 5, 4, 3, 1)
      .map((x: Int) => 0.5 * predict(month, x, lat, lng, contextSQL, model))

    val byTypeK = Array(3, 4, 2, 1, 5)
      .map((x: Int) => 0.5 * predict(month, x, lat, lng, contextSQL, model))

    val historical = Array(4, 2, 3, 5, 1)
      .map((x: Int) => 0.5 * predict(month, x, lat, lng, contextSQL, model) + scala.math.abs(scala.math.cos((lat + lng + month) * x * 2) + scala.math.cos((lat + lng + month) * x * 3)))

    Map(
      "by_point" -> byPointK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
      "by_type" -> byTypeK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
      "historical" -> historical.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap
    )
  }
}