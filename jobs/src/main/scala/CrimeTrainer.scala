import java.io.FileOutputStream

import io.hydrosphere.mist.lib.{MistJob, SQLSupport}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import java.nio.file.{Files, Paths}

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SaveMode

object CrimeTrainer extends MistJob with SQLSupport {

  def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

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
