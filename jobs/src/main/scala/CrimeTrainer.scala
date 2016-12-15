import io.hydrosphere.mist.lib.{MLMistJob, PipelineLoader, SQLSupport}
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import java.nio.file.{Files, Paths}

import org.apache.spark.ml.{Pipeline, PredictionModel, Transformer}

import scala.math.BigInt

object CrimeTrainer extends MLMistJob with SQLSupport {

//  override def train(params: Map[String, Any]): Map[String, Any] = ???

  import io.hydrosphere.mist.ml.HackedSpark._

  def predict(month: Int, x: Int, lat: Double, lon: Double, pipeline: Array[Transformer]): Double = {

    if (Files.exists(Paths.get(s"model_${lat}_$lon"))) {
      val predictions = pipeline.map { (t: Transformer) =>
        t.asInstanceOf[PredictionModel[Vector, MultilayerPerceptronClassificationModel]].transform(Array(Vectors.dense(month.toDouble / 12.0)))
      }
      val predictedCrime = predictions(0)(0)
      predictedCrime.toString.toDouble / 100 + scala.math.abs(scala.math.sin((lat + lon + month) * x))
    }
    else 0.0
  }

  override def serve(parameters: Map[String, Any]): Map[String, Any] = {

    val lat = parameters("lat").asInstanceOf[String].toDouble
    val lng = parameters("lng").asInstanceOf[String].toDouble
    val month = parameters("month").asInstanceOf[BigInt].intValue

    val pipeline = PipelineLoader.load(s"model_${lat}_$lng")

    val byPointK = Array(2, 5, 4, 3, 1)
      .map((x: Int) => 0.5 * predict(month, x, lat, lng, pipeline))

    val byTypeK = Array(3, 4, 2, 1, 5)
      .map((x: Int) => 0.5 * predict(month, x, lat, lng, pipeline))

    val historical = Array(4, 2, 3, 5, 1)
      .map((x: Int) => 0.5 * predict(month, x, lat, lng, pipeline) + scala.math.abs(scala.math.cos((lat + lng + month) * x * 2) + scala.math.cos((lat + lng + month) * x * 3)))

    Map(
      "by_point" -> byPointK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
      "by_type" -> byTypeK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
      "historical" -> historical.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap
    )
  }

//  override def serve(params: Map[String, Any]): Map[String, Any] = {
//    // val pipeline = LocalPipeline.load()
//    // pipeline.predict(/* params */)
//    
//    Map.empty[String, Any]
//  }

  def train(parameters: Map[String, Any]): Map[String, Any] = {

    val contextSQL = session.sqlContext

    val lat = parameters("lat").asInstanceOf[String].toDouble
    val lng = parameters("lng").asInstanceOf[String].toDouble
    val month = parameters("month").asInstanceOf[BigInt].intValue

    if (Files.exists(Paths.get(s"model_${lat}_$lng/data/_SUCCESS"))) {
      return Map("result" -> "already trained")
    }

    var crimeCollection = collection.mutable.ArrayBuffer[(Double, org.apache.spark.ml.linalg.Vector)]()
    for (x <- 1 to 10) {
      for (monthT <- 1 to 12) {
        crimeCollection += (((scala.math.abs(scala.math.cos(lat + lng + monthT))*100).toInt.toDouble, Vectors.dense(monthT.toDouble / 12.0)))
      }
    }

    val data = contextSQL.createDataFrame(crimeCollection).toDF("label", "features")

    data.show(15)

    val splits = data.randomSplit(Array(0.9, 0.1), seed = 1234L)
    val train = splits(0)
    val test = splits(1)
    val layers = Array[Int](1, 42, 100)

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(64)
      .setSeed(1234L)
      .setMaxIter(300)

    val pipeline = new Pipeline().setStages(Array(trainer))

    val model = pipeline.fit(train)

    model.write.overwrite().save(s"model_${lat}_$lng")

    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    println("accuracy " + evaluator.evaluate(predictionAndLabels))

    Map("result" -> "trained")
  }
}
