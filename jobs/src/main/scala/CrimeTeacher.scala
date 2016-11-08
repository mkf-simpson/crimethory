import io.hydrosphere.mist.lib.{MistJob, SQLSupport}
import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object CrimeTeacher extends MistJob with SQLSupport {

  def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

    val contextSQL = session.sqlContext
    val context = session.sparkContext

    var crimeCollection = collection.mutable.ArrayBuffer[(Double, org.apache.spark.ml.linalg.Vector)]()
    for (iter <- 1 to 5) {
      for (geo <- 1 to 999) {
        for (x <- 1 to 10) {
          for (monthT <- 1 to 12) {
            crimeCollection += (((scala.math.abs(scala.math.cos((monthT) * x) + scala.math.cos((monthT) * x * 2) + scala.math.cos((monthT) * x * 3)) * 10).toInt.toDouble, Vectors.dense(monthT.toDouble / 12.0, x.toDouble / 10.0, geo.toDouble / 1000.0)))
          }
        }
      }
    }

    val data = contextSQL.createDataFrame(crimeCollection).toDF("label", "features")

    data.show(15)

    val splits = data.randomSplit(Array(0.9, 0.1), seed = 1234L)
    val train = splits(0)
    val test = splits(1)
    val layers = Array[Int](3, 42, 100)

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(64)
      .setSeed(1234L)
      .setMaxIter(300)

    val model = trainer.fit(train)

    model.write.overwrite().save("model")

    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    println("accuracy " + evaluator.evaluate(predictionAndLabels))

    Map("result" -> "Teach Success")
  }
}