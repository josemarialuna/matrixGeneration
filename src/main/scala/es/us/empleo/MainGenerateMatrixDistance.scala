package es.us.empleo

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


/**
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object MainGenerateMatrixDistance {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Empleo Spark")
      .setMaster("yarn-cluster")

    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val fileOriginal = "C:\\datasets\\trabajadores.csv"
    val fileOriginalMin = "C:\\datasets\\trabajadores-min.csv"
    val file100 = "C:\\datasets\\trabajadores100.csv"
    val file4000 = "C:\\datasets\\trabajadores4000.csv"

    var origen: String = fileOriginalMin
    var destino: String = Utils.whatTimeIsIt()
    var numPartitions = 4 // cluster has 25 nodes with 4 cores. You therefore need 4 x 25 = 100 partitions.

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

    val dataRDD = skippedData.map(s => s.split(';').map(_.toFloat)).cache()

    // assuming dataRDD has type RDD[Array[Double]] and each Array has at least 4 items:
    val result = dataRDD
      .keyBy(_ (0).toInt)
      .mapValues(arr => Map(arr(1).toInt -> arr(2) / arr(3) * 100))
      .reduceByKey((a, b) => a ++ b)
    //.filter(_._1 < 100)


    val porfin = result.mapValues { y =>
      var rowVector = Vector.fill(totalY + 1) {
        0.0f
      }
      for (z <- 1 to totalY) {
        if (y.exists(_._1 == z)) {
          rowVector = rowVector.updated(z, y(z))
        }
      }
      rowVector
    }.partitionBy(partCustom)


    //porfin.mapValues(_.mkString(";")).saveAsTextFile(destino + "porfin")

    //Calculo de las distancias entre Workers
    val distances = porfin.cartesian(porfin)
      //.filter { case (a, b) => a._1 < b._1 }
      .map { case (x, y) =>
      var totalDist = 0.0f
      for (z <- 0 to totalY) {
        var minAux = 0.0f
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
      //(x._1, y._1, 100.0f - totalDist)
      x._1 + "," + y._1 + "," + (100.0f - totalDist)
    }

    distances.coalesce(1, shuffle = true)
      .saveAsTextFile(destino + "distancesDouble" + Utils.whatTimeIsIt())

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
            for (z <- 0 to totalX) {
              if (y.exists(_._1 == z)) {
                rowVector = rowVector.updated(z, y(z))
              }
            }
            rowVector
          }
          .sortByKey()
        matrix.map(_._2.mkString(",")).saveAsTextFile(destino + "MatrixDistance")
    */
    sc.stop()

  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    if (s.isEmpty) 0 else s.toDouble
  }

}
