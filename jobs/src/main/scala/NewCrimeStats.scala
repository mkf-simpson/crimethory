import java.io.File
import java.nio.charset.{Charset, CharsetDecoder}

import io.hydrosphere.mist.lib.{MistJob, SQLSupport}
import io.circe._
import io.circe.parser._
import io.circe.generic.semiauto._
import cats.syntax.either._
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel

import scala.math.BigInt
import scala.io.Source
import org.apache.spark.ml.linalg.{Vector, Vectors}
import java.nio.file.Paths
import java.nio.file.Files
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.{PredictionModel, Transformer}

import scala.reflect.ClassTag
import scala.reflect._
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import parquet.hadoop.{ParquetFileReader, ParquetReader}
import parquet.hadoop.api.{InitContext, ReadSupport}
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.io.api._
import parquet.schema.{GroupType, MessageType, OriginalType, Type}

import scala.collection.immutable.{HashMap, Iterable}
import scala.collection.mutable.ListBuffer
import parquet.format.converter.ParquetMetadataConverter.NO_FILTER


class SimpleRecordConverter(schema: GroupType, name: String, parent: SimpleRecordConverter) extends GroupConverter {
  val UTF8: Charset = Charset.forName("UTF-8")
  val UTF8_DECODER: CharsetDecoder = UTF8.newDecoder()

  var converters: Array[Converter] = schema.getFields.map(createConverter).toArray[Converter]

  var record: SimpleRecord = _

  private def createConverter(field: Type): Converter = {
    if (field.isPrimitive) {
      val originalType = field.getOriginalType
      originalType match {
        case OriginalType.UTF8 => return new StringConverter(field.getName)
        case _ => Unit
      }

      return new SimplePrimitiveConverter(field.getName)
    }

    new SimpleRecordConverter(field.asGroupType(), field.getName, this)
  }

  override def getConverter(i: Int): Converter = {
    converters(i)
  }

  override def start(): Unit = {
    record = new SimpleRecord()
  }

  override def end(): Unit = {
    if (parent != null) {
      parent.record.add(name, record)
    }
  }

  private class StringConverter(name: String) extends SimplePrimitiveConverter(name) {
    override def addBinary(value: Binary): Unit = {
      record.add(name, value.toStringUsingUTF8)
    }
  }

  private class SimplePrimitiveConverter(name: String) extends PrimitiveConverter {
    override def addBinary(value: Binary): Unit = {
      val bytes = value.getBytes
      if (bytes == null) {
        record.add(name, null)
        return
      }

      try {
        val buffer = UTF8_DECODER.decode(value.toByteBuffer)
        record.add(name, buffer.toString)
      } catch {
        case _: Throwable => Unit
      }
    }

    override def addBoolean(value: Boolean) {
      record.add(name, value)
    }

    override def addDouble(value: Double) {
      record.add(name, value)
    }

    override def addFloat(value: Float) {
      record.add(name, value)
    }

    override def addInt(value: Int) {
      record.add(name, value)
    }

    override def addLong(value: Long) {
      record.add(name, value)
    }
  }
}

class SimpleRecordMaterializer(schema: MessageType) extends RecordMaterializer[SimpleRecord] {

  val root: SimpleRecordConverter = new SimpleRecordConverter(schema, null, null)

  override def getRootConverter: GroupConverter = root

  override def getCurrentRecord: SimpleRecord = root.record
}

class SimpleReadSupport extends ReadSupport[SimpleRecord] {
  override def prepareForRead(configuration: Configuration, map: util.Map[String, String], messageType: MessageType, readContext: ReadContext): RecordMaterializer[SimpleRecord] = {
    new SimpleRecordMaterializer(messageType)
  }

  override def init(context: InitContext): ReadContext = {
    new ReadContext(context.getFileSchema)
  }
}

case class NameValue(name: String, value: Any) {
  override def toString: String = {
    s"$name: ${value.toString}"
  }
}

class SimpleRecord {

  implicit class PrettyPrintMap[K, V](val map: Map[K, V]) {
    def prettyPrint: PrettyPrintMap[K, V] = this

    override def toString: String = {
      val valuesString = toStringLines.mkString("\n")

      "Map (\n" + valuesString + "\n)"
    }

    def toStringLines: Iterable[String] = {
      map
        .flatMap { case (k, v) => keyValueToString(k, v) }
        .map(indentLine)
    }

    def keyValueToString(key: K, value: V): Iterable[String] = {
      value match {
        case v: Map[_, _] => Iterable(key + " -> Map (") ++ v.prettyPrint.toStringLines ++ Iterable(")")
        case x => Iterable(key + " -> " + x.toString)
      }
    }

    def indentLine(line: String): String = {
      "\t" + line
    }
  }

  val values: scala.collection.mutable.ArrayBuffer[NameValue] = scala.collection.mutable.ArrayBuffer.empty[NameValue]

  def add(name: String, value: Any): Unit = {
    values add NameValue(name, value)
  }

  def struct(acc: HashMap[String, Any], schema: Type, parent: NameValue = null): HashMap[String, Any] = {
    var map = acc
    val eitherSchema = if (schema.isPrimitive) {
      Left(schema.asPrimitiveType())
    } else {
      Right(schema.asGroupType())
    }
    for (nameValue <- values) {
      val value = nameValue.value
      val name = nameValue.name

      eitherSchema match {
        case Left(_) => map += name -> value
        case Right(parentType) =>
          val subSchema = parentType.getType(name)
          if (subSchema.isPrimitive) {
            map += name -> value
          } else {
            parentType.getOriginalType match {
              case OriginalType.LIST =>
                val parentName = parent.name
                map += parentName -> value.asInstanceOf[SimpleRecord].struct(map.getOrElse(parentName, List.empty[Any]).asInstanceOf[List[Any]])
              case _ =>
                subSchema.asGroupType().getOriginalType match {
                  case OriginalType.LIST =>
                    map ++= value.asInstanceOf[SimpleRecord].struct(map.getOrElse(name, HashMap.empty[String, Any]).asInstanceOf[HashMap[String, Any]], subSchema, nameValue)
                  case _ =>
                    map += name -> value.asInstanceOf[SimpleRecord].struct(map.getOrElse(name, HashMap.empty[String, Any]).asInstanceOf[HashMap[String, Any]], subSchema, nameValue)
                }
            }
          }
      }
    }
    map
  }

  def struct(acc: List[Any]): List[Any] = {
    val list = acc.to[ListBuffer]
    for (nameValue <- values) {
      val value = nameValue.value
      val name = nameValue.name

      if (name.equals("element") && !classOf[SimpleRecord].isAssignableFrom(value.getClass)) {
        list += value
      }
    }
    list.to[List]
  }

  def prettyPrint(schema: Type): Unit = {
    println(struct(HashMap.empty[String, Any], schema).prettyPrint)
  }
}

object ModelDataReader {

  def parse(path: String): HashMap[String, Any] = {
    val parquetFile = new File(path).listFiles.filter(_.isFile).toList.filter(_.getAbsolutePath.endsWith("parquet")).head.getAbsolutePath
    val conf: Configuration = new Configuration()
    val metaData = ParquetFileReader.readFooter(conf, new Path(parquetFile), NO_FILTER)
    val schema: MessageType = metaData.getFileMetaData.getSchema

    val reader: ParquetReader[SimpleRecord] = ParquetReader.builder[SimpleRecord](new SimpleReadSupport(), new Path(path)).build()
    var result = HashMap.empty[String, Any]
    try {
      var value = reader.read()
      while (value != null) {
        println(s"new value: ${value.values.length} rows")
        value.prettyPrint(schema)
        result ++= value.struct(HashMap.empty[String, Any], schema)
        value = reader.read()
      }
      result
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }
}

object HackedSpark {
  implicit class HackedPipeline[FeaturesType:ClassTag, M <: PredictionModel[FeaturesType, M]](val predictor: PredictionModel[Vector, MultilayerPerceptronClassificationModel]) {
    def transform(features: Array[FeaturesType]): Array[Double] = {
      val method = predictor.getClass.getMethod("predict", classTag[FeaturesType].runtimeClass.asInstanceOf[Class[FeaturesType]])
      method.setAccessible(true)
      features map { (feature: FeaturesType) =>
        method.invoke(predictor, feature.asInstanceOf[Vector]).asInstanceOf[Double]
      }
    }
  }
}

object ModelLoader {

  case class PipelineParameters(className: String, timestamp: Long, sparkVersion: String, uid: String, paramMap: Map[String, List[String]])
  //  {
  //    "class":"org.apache.sparzk.ml.PipelineModel",
  //    "timestamp":1480604356248,
  //    "sparkVersion":"2.0.0",
  //    "uid":"pipeline_5a99d584b039",
  //    "paramMap": {
  //      "stageUids":["mlpc_c6d88c0182d5"]
  //    }
  //  }

  case class StageParameters(className: String, timestamp: Long, sparkVersion: String, uid: String, paramMap: Map[String, String])
  //  {
  //    "class": "org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel",
  //    "timestamp": 1480604356363,
  //    "sparkVersion": "2.0.0",
  //    "uid": "mlpc_c6d88c0182d5",
  //    "paramMap": {
  //      "featuresCol": "features",
  //      "predictionCol": "prediction",
  //      "labelCol": "label"
  //    }
  //  }


  implicit val pipelineParametersDecoder: Decoder[PipelineParameters] = deriveDecoder
  implicit val stageParametersDecoder: Decoder[StageParameters] = deriveDecoder

  def get(path: String): Array[Transformer] = {
    val metadata = Source.fromFile(s"$path/metadata/part-00000").mkString.replace("class", "className")
    println(s"$path/metadata/part-00000")
    println(metadata)
    val parsed = parse(metadata).getOrElse(Json.Null).as[PipelineParameters]
    parsed match {
      case Left(failure) =>
        println(s"FAILURE while parsing pipeline metadata json: $failure")
        null
      case Right(pipelineParameters: PipelineParameters) =>
        pipelineParameters.paramMap("stageUids").zipWithIndex.toArray.map {
          case (uid: String, index: Int) =>
            println(s"reading $uid stage")
            println(s"$path/stages/${index}_$uid/metadata/part-00000")
            val modelMetadata = Source.fromFile(s"$path/stages/${index}_$uid/metadata/part-00000").mkString.replace(""""class"""", """"className"""")
            println(modelMetadata)
            val parsedModel = parse(modelMetadata).getOrElse(Json.Null).as[StageParameters]
            parsedModel match {
              case Left(failure) =>
                println(s"FAILURE while parsing stage $uid metadata json: $failure")
                null
              case Right(stageParameters: StageParameters) =>
                println(s"Stage class: ${stageParameters.className}")
                val data = ModelDataReader.parse(s"$path/stages/${index}_$uid/data/")

                val cls = Class.forName(stageParameters.className)
                val constructor = cls.getDeclaredConstructor(classOf[String], classOf[Array[Int]], classOf[Vector])

                constructor.setAccessible(true)
                val now = System.currentTimeMillis
                val res = constructor.newInstance(uid, data("layers").asInstanceOf[List[Int]].to[Array], Vectors.dense(data("weights").asInstanceOf[HashMap[String, Any]]("values").asInstanceOf[List[Double]].toArray)).asInstanceOf[MultilayerPerceptronClassificationModel]
                println(s"instantiating: ${System.currentTimeMillis - now}ms")
                res
            }
        }
    }
  }

}

object NewCrimeStats extends MistJob with SQLSupport {

  import HackedSpark._

  def predict(month: Int, x: Int, lat: Double, lon: Double, pipeline: Array[Transformer]): Double = {

    if (Files.exists(Paths.get(s"model_${lat}_$lon"))) {
      val predictions = pipeline.map { (t: Transformer) =>
        t.asInstanceOf[PredictionModel[Vector, MultilayerPerceptronClassificationModel]].transform(Array(Vectors.dense(month.toDouble/12.0)))
      }
      val predictedCrime = predictions(0)(0)
      predictedCrime.toString.toDouble / 100 + scala.math.abs(scala.math.sin((lat + lon + month) * x))
    }
    else 0.0
  }

  def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

    val lat = parameters("lat").asInstanceOf[String].toDouble
    val lng = parameters("lng").asInstanceOf[String].toDouble
    val month = parameters("month").asInstanceOf[BigInt].intValue

    val pipeline = ModelLoader.get(s"model_${lat}_$lng")

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
}
