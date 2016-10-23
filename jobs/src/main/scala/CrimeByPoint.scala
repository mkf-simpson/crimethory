import io.hydrosphere.mist.lib.MistJob
import scala.math.BigInt

object CrimeByPoint extends MistJob {

    def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
        val lat = parameters("lat").asInstanceOf[BigInt].doubleValue
        val lng = parameters("lng").asInstanceOf[BigInt].doubleValue
        val month = parameters("month").asInstanceOf[BigInt].intValue

        val initialK = Array(2, 5, 4, 3, 1)
            .map((x: Int) => scala.math.abs(scala.math.sin((lat + lng + month) * x)));

        initialK.zipWithIndex.map{
            case (s, i) => i.toString -> s
        } toMap
    }

}