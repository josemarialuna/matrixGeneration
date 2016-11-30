package es.us.empleo

import org.apache.spark
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

/**
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object MainGenerateMatrixDistanceDF {
  def main(args: Array[String]) {

    //set up the spark configuration and create contexts
    val conf = new SparkConf()
      .setAppName("Empleo Spark")
      .setMaster("yarn-cluster")

    val sc = new SparkContext(conf)

    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val fileOriginal = "C:\\datasets\\trabajadores.csv"
    val fileOriginalMin = "C:\\datasets\\trabajadores-min.csv"
    val file100 = "C:\\datasets\\trabajadores100.csv"
    val file4000 = "C:\\datasets\\trabajadores4000.csv"

    var origen: String = fileOriginal
    var destino: String = Utils.whatTimeIsIt()
    var numPartitions = 16 //3 or 4 times the number of CPUs in your cluster

    val totalX = 9170
    val totalY = 9280
    //val totalX = 9
    //val totalY = 9


    if (args.size > 2) {
      origen = args(0)
      destino = args(1)
      numPartitions = args(2).toInt
    }

    val partCustom = new HashPartitioner(numPartitions)

    val data = sc.textFile(origen)
    //It skips the first line
    val skippedData = data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val dataRDD = skippedData.map(s => s.split(';').map(_.toDouble)).cache()

    // assuming dataRDD has type RDD[Array[Double]] and each Array has at least 4 items:
    val result = dataRDD
      .keyBy(_ (0).toInt)
      .mapValues(arr => Map(arr(1).toInt -> arr(2) / arr(3) * 100))
      .reduceByKey((a, b) => a ++ b)


    val porfin = result.mapValues { y =>
      var rowVector = Vector.fill(totalY + 1) {
        0.0
      }
      for (z <- 1 to totalY) {
        if (y.exists(_._1 == z)) {
          rowVector = rowVector.updated(z, y(z))
        }
      }
      rowVector
    }.cache()

    //porfin.mapValues(_.mkString(";")).saveAsTextFile("porfin")

    case class Distancia(idW: Int, idJ: Int, distancia: Double) extends Serializable
/*
    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._
    //Calculo de las distancias entre Workers
    val distances = porfin.cartesian(porfin)
      .filter { case (a, b) => a._1 < b._1 }
      .partitionBy(partCustom)
      .map { case (x, y) =>
        var totalDist = 0.0
        for (z <- 1 to totalY) {
          var minAux = 0.0
          try {
            val line = x._2(z)
            val linePlus = y._2(z)
            //El mínimo se suma a totalDist
            if (line < linePlus) {
              minAux = line
            } else {
              minAux = linePlus
            }
          } catch {
            case e: Exception => null
          } finally {
            totalDist += minAux
          }
        }
        //id_W1, id_W2, dist
        Distancia(x._1, y._1, 100.0 - totalDist)
      }.toDF()

    distances.saveAsTextFile("distances")

    /*
        //Mapeado de distancias para tener un output: RDD[idW1, Array(ids_Wx)]
        val matrix = distances
          .keyBy(_._1)
          .mapValues(arr => Map(arr._2 -> arr._3))
          .reduceByKey((a, b) => a ++ b)
          .mapValues { y =>
            var rowVector = Vector.fill(totalX + 1) {
              0.0
            }
            for (z <- 1 to totalX) {
              if (y.exists(_._1 == z)) {
                rowVector = rowVector.updated(z, y(z))
              }
            }
            rowVector
          }
          .sortByKey()

        matrix.map(_._2.mkString(",")).saveAsTextFile(destino + "MatrixDistance")
    */
*/
    sc.stop()

  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    if (s.isEmpty) 0 else s.toDouble
  }

}

