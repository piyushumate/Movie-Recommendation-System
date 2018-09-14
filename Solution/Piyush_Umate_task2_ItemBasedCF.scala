import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap


object ItemBasedCF {
  var user_movie_rating_dict = scala.collection.mutable.Map[(Int, Int), Double]()
  var similarity_data_dict = scala.collection.mutable.Map[(Int, Int), Double]()
  var movie_user_ids_dict = scala.collection.mutable.Map[Int, Set[Int]]()
  var user_movie_ids_dict = scala.collection.mutable.Map[Int, Set[Int]]()
  var predictions = scala.collection.mutable.Map[(Int, Int), Double]()
  var movie_rating_dict = Map[Any, Double]()
  val output_file = "Piyush_Umate_ItemBasedCF.txt"

  def compute_range(value: Double): Int = {
    if(value >= 0.0 && value < 1.0) {
      0
    } else if(value >= 1.0 && value < 2.0) {
      1
    } else if(value >= 2.0 && value < 3.0){
      2
    } else if (value >= 3.0 && value < 4.0){
      3
    } else {
      4
    }

  }

  def average(doubles: List[Double]): Double = {
    doubles.sum/doubles.length
  }

  def user_movie_map(line: String, regex: String=","): ((Int, Int), Double) = {
    var l = line.split(regex)
    if (l.size == 2) {
      ((l(0).toInt, l(1).toInt), -1.0)
    } else {
      ((l(0).toInt, l(1).toInt), l(2).toDouble)
    }
  }

  def compute_predictions(user_id_1: Int, movie_id_1: Int) : Double = {
    val movie_ids = (user_movie_ids_dict(user_id_1) -- Set(movie_id_1)).toList
    var numerator = 0.0
    var denominator = 0.0
    var movie_1 = 0
    var movie_2 = 0
    for(movie_id_2 <- movie_ids) {
      if(movie_id_2 < movie_id_1) {
        movie_1 = movie_id_2
        movie_2 = movie_id_1
      } else {
        movie_1 = movie_id_1
        movie_2 = movie_id_2
      }
      if(similarity_data_dict.contains((movie_1, movie_2))) {
        val w = similarity_data_dict(movie_1, movie_2)
        denominator += Math.abs(w)
        numerator += (user_movie_rating_dict((user_id_1, movie_id_2))*w)
      }

    }

    if(denominator == 0.0) {
      movie_rating_dict.getOrElse(movie_id_1, 3.5)
    } else {
      val rating = numerator/denominator
      if(rating < 0.0) {
        0.0
      } else if(rating > 5.0) {
        5.0
      } else {
        rating
      }
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

    val training_data = data.subtractByKey(testing_data).persist()
    user_movie_rating_dict = collection.mutable
      .Map(training_data.collect().toSeq: _*)

    val similarity_data = spark_context.textFile(args(2))
      .map(line => user_movie_map(line, ", "))
      .persist()

    similarity_data_dict = collection.mutable
        .Map(similarity_data.collect().toSeq: _*)

    movie_rating_dict = training_data.map(
      user_movie_rating => (user_movie_rating._1._2,user_movie_rating._2)
    ).groupByKey().map(x => (x._1, average(x._2.toList))).collect().toMap

//    similarity_data.take(5).foreach(println)

    val user_movie_ids = training_data
      .keys.groupByKey()
      .mapValues(_.toSet)
      .persist()

    val movie_user_ids = training_data
      .keys.map(_.swap)
      .groupByKey()
      .mapValues(_.toSet)
      .persist()

    val user_ids = user_movie_ids.keys
    movie_user_ids_dict = collection.mutable
      .Map(movie_user_ids.collect().toSeq: _*)

    user_movie_ids_dict = collection.mutable
      .Map(user_movie_ids.collect().toSeq: _*)

    val testing_ids = testing_data.keys.collect()


    val data_dict = collection.mutable
      .Map(data.collect().toSeq : _*)

    val writer = new PrintWriter(new File(output_file))
    val last_index = testing_ids.size - 1

    for (((user_id, movie_id), index)<- testing_ids.zipWithIndex) {
      val rating = compute_predictions(
        user_id, movie_id
      )
      predictions((user_id, movie_id)) = rating
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

    val absolute_difference = spark_context.parallelize(predictions.toSeq).map(
      t => (t._1, Math.abs(t._2 - data_dict(t._1)))
    ).persist()

    var baseline = absolute_difference.map(t => (compute_range(t._2),1)).countByKey()
    baseline = ListMap(baseline.toSeq.sortBy(_._1):_*)
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

    val RMSE = Math.sqrt(absolute_difference.map(x => Math.pow(x._2,2)).mean())
    println("RMSE: "+ RMSE)
    println("Time: " + (System.currentTimeMillis() - t0)/1000 + "sec")


//    println(predictions)

  }
}
