import io.hydrosphere.mist.lib.{MistJob, SQLSupport}
import scala.math.BigInt

import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object CrimeByPointNeuro extends MistJob with SQLSupport {

  def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

    val contextSQL = session.sqlContext
    val context = session.sparkContext

    val lat = parameters("lat").asInstanceOf[String].toDouble
    val lng = parameters("lng").asInstanceOf[String].toDouble
    val month = parameters("month").asInstanceOf[BigInt].intValue

    var crimeCollection = collection.mutable.ArrayBuffer[(Double, org.apache.spark.ml.linalg.Vector)]()
    for(x <- 1 to 5) {
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

    val model = trainer.fit(train)

    val result = model.transform(test)

    result.show(15)

    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    println("accuracy " + evaluator.evaluate(predictionAndLabels))

    val featureReq = Seq((1.0, Vectors.dense(month.toDouble/12.0)))

    val requestData = contextSQL.createDataFrame(featureReq).toDF("label", "features")

    requestData.show()

    val prediction = model.transform(requestData)

    val predictedCrime = prediction.select("prediction").head()
    if(predictedCrime.size > 0 )
       println("crime: ", predictedCrime.toString)

    val crimePoint = predictedCrime(0).toString.toDouble


    val byPointK = Array(2, 5, 4, 3, 1)
      .map((x: Int) => 0.5 * (crimePoint/100.0 + scala.math.abs(scala.math.sin((lat + lng + month) * x))));

    val byTypeK = Array(3, 4, 2, 1, 5)
      .map((x: Int) => 0.5 * (crimePoint/100.0 + scala.math.abs(scala.math.sin((lat + lng + month) * x))));

    val historical = Array(4, 2, 3, 5, 1)
      .map((x: Int) => 0.5 * (crimePoint/100.0 + scala.math.abs(scala.math.cos((lat + lng + month) * x) + scala.math.cos((lat + lng + month) * x * 2) + scala.math.cos((lat + lng + month) * x * 3))));

    Map(
      "by_point" -> byPointK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
      "by_type" -> byTypeK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
      "historical" -> historical.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap
    )

  }

}