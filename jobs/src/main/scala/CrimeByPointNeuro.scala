
import io.hydrosphere.mist.lib.{MistJob, SQLSupport}

import scala.math.BigInt
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SQLContext

import java.nio.file.Paths
import java.nio.file.Files

object CrimeByPointNeuro extends MistJob with SQLSupport {

  def predict(month: Int, x: Int, lat: Double, lon: Double, contextSQL: SQLContext): Double= {
    if (Files.exists(Paths.get("model/data/_SUCCESS"))) {
      val featureReq = Seq((1.0, Vectors.dense(month.toDouble / 12.0, x / 10.0, (math.abs(10.0*(lat+lon)).toDouble - math.abs(10.0*(lat+lon)).toInt))))
      val requestData = contextSQL.createDataFrame(featureReq).toDF("label", "features")

      val model = MultilayerPerceptronClassificationModel.load("model")
      val prediction = model.transform(requestData)
      val predictedCrime = prediction.select("prediction").head()
      if (predictedCrime.size > 0)
        println("crime: ", predictedCrime.toString)

      val crimePoint = predictedCrime(0).toString.toDouble
      return crimePoint
    }
    else
      return 0.0
  }

  def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

    val contextSQL = session.sqlContext
    val context = session.sparkContext

    val lat = parameters("lat").asInstanceOf[String].toDouble
    val lng = parameters("lng").asInstanceOf[String].toDouble
    val month = parameters("month").asInstanceOf[BigInt].intValue

    val byPointK = Array(2, 5, 4, 3, 1)
      .map((x: Int) => predict(month, x, lat, lng, contextSQL));

    val byTypeK = Array(3, 4, 2, 1, 5)
      .map((x: Int) => predict(month, x, lat, lng, contextSQL));

    val historical = Array(4, 2, 3, 5, 1)
      .map((x: Int) => predict(month, x, lat, lng, contextSQL));

    Map(
      "by_point" -> byPointK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
      "by_type" -> byTypeK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
      "historical" -> historical.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap
    )
  }
}