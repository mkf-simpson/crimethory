import io.hydrosphere.mist.lib.MistJob
import scala.math.BigInt

object CrimeByPoint extends MistJob {

    def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
        val lat = parameters("lat").asInstanceOf[String].toDouble
        val lng = parameters("lng").asInstanceOf[String].toDouble
        val month = parameters("month").asInstanceOf[BigInt].intValue

        val byPointK = Array(2, 5, 4, 3, 1)
            .map((x: Int) => scala.math.abs(scala.math.sin((lat + lng + month) * x)));

        val byTypeK = Array(3, 4, 2, 1, 5)
            .map((x: Int) => scala.math.abs(scala.math.sin((lat + lng + month) * x)));

        val historical = Array(4, 2, 3, 5, 1)
            .map((x: Int) => scala.math.abs(scala.math.cos((lat + lng + month) * x) + scala.math.cos((lat + lng + month) * x * 2) + scala.math.cos((lat + lng + month) * x * 3)));

        Map(
            "by_point" -> byPointK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
            "by_type" -> byTypeK.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap,
            "historical" -> historical.zipWithIndex.map({ case (s, i) => i.toString -> s }).toMap
        )

    }

}