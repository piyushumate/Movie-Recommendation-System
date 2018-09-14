import java.io.{File, PrintWriter}


import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

import scala.collection.immutable.ListMap

object ModelBasedCF {
  val output_file = "Piyush_Umate_ModelBasedCF.txt"
  def user_movie_map(line: String): ((Int, Int), Double) = {
    var l = line.split(",")
    if (l.size == 2) {
      ((l(0).toInt, l(1).toInt), -1.0)
    } else {
      ((l(0).toInt, l(1).toInt), l(2).toDouble)
    }
  }

  def compute_range(value: Double): Int = {
    if (value >= 0.0 && value < 1.0) {
      0
    } else if(value >= 1.0 && value < 2.0) {
      1
    } else if(value >= 2.0 && value < 3.0) {
      2
    } else if(value >= 3.0 && value < 4.0) {
      3
    } else {
      4
    }
  }

  def main(args: Array[String]): Unit = {
    val t0 = System.currentTimeMillis()
    val conf = new SparkConf()
    val spark_context = new SparkContext(conf)
    spark_context.setLogLevel("WARN")
    val test_file = spark_context.textFile(args(1))
    val test_file_header = test_file.first()
    val testing_data = test_file.filter(line => line != test_file_header)
      .map(line => user_movie_map(line))
      .persist()

    var data_stringy = spark_context.textFile(args(0))
    val header = data_stringy.first()
    val data = data_stringy.filter(line => line != header)
        .map(line => user_movie_map(line))
        .persist()

    val training_data = data.subtractByKey(testing_data)
      .map(_ match {case ((user_id, movie_id), rating) => Rating(user_id, movie_id, rating)})
      .persist()

    val rank = 10
    val numIterations = 12
    val lambda = 0.1

    val model = ALS.train(
      training_data, rank, numIterations, lambda
    )

    val predictions = model.predict(testing_data.keys).map { case Rating(user, product, rate) =>
      ((user, product), rate)}.sortByKey()

    val ratesAndPreds = data.map(identity).join(predictions)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    val RMSE = math.sqrt(MSE)

//    println(RMSE)

    var baseline = ratesAndPreds.map{ case ((user, product), (r1, r2)) =>
      (compute_range(math.abs(r1 - r2)),1)}.countByKey()

    baseline = ListMap(baseline.toSeq.sortBy(_._1):_*)


    val predictions_dict = predictions.collect()
    val writer = new PrintWriter(new File(output_file))
    val last_index = predictions_dict.size - 1

    for ((((user_id, movie_id),rating), index) <- predictions_dict.zipWithIndex) {

      val formatted_string = Array(
        user_id.toString(),
        movie_id.toString(),
        rating.toString()
      ).mkString(", ")

      if (index != last_index) {
        writer.write(s"$formatted_string\n")
      } else {
        writer.write(s"$formatted_string")
      }
    }
    writer.close()

    for((k,v)<-baseline) {
      if(k == 0) {
        println(">=0 and <1: "+ v)
      } else if (k == 1) {
        println(">=1 and <2: "+ v)
      } else if(k == 2) {
        println(">=2 and <3: "+ v)
      } else if(k == 3) {
        println(">=3 and <4: "+ v)
      } else {
        println(">=4: "+ v)
      }
    }
//    println(baseline)

    println("RMSE: "+ RMSE)
    println("Time: " + (System.currentTimeMillis() - t0)/1000 + "sec")
  }
}
