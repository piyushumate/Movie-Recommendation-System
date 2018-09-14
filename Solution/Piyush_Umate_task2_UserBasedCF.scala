import java.io.{File, PrintWriter}


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import util.control.Breaks._

object UserBasedCF {
  var pearson_dict = scala.collection.mutable.Map[(Int, Int), Double]()
  var movie_user_ids_dict = scala.collection.mutable.Map[Int, Set[Int]]()
  var user_movie_ids_dict = scala.collection.mutable.Map[Int, Set[Int]]()
  var user_averages = scala.collection.mutable.Map[Int, Double]()
  var user_movie_rating_dict = scala.collection.mutable.Map[(Int, Int), Double]()
  var predictions = scala.collection.mutable.Map[(Int, Int), Double]()
  val output_file = "Piyush_Umate_UserBasedCF.txt"

  def user_movie_map(line: String): ((Int, Int), Double) = {
    var l = line.split(",")
    if (l.size == 2) {
      ((l(0).toInt, l(1).toInt), -1.0)
    } else {
      ((l(0).toInt, l(1).toInt), l(2).toDouble)
    }
  }

  def average(list: List[Double]): Double = {
    assert(list.length > 0)
    list.sum / list.size
  }

  def get_user_movie_ratings(user_id: Int, movies: Set[Int]): List[Double] = {
    movies.toList.map(movie => user_movie_rating_dict((user_id, movie)))
  }

  def get_user_movie_average(user_id: Int, movie_id: Int): Double = {
    average(get_user_movie_ratings(
      user_id, user_movie_ids_dict(user_id) -- Set(movie_id)
    ))
  }

  def compute_predictions(user_id_1: Int, movie_id: Int): Double = {
    if (movie_user_ids_dict.contains(movie_id)) {
      val user_ids = (movie_user_ids_dict(movie_id) -- Set(user_id_1)).toList

      var denominator = 0.0
      var numerator = 0.0

      for (user_id_2 <- user_ids) {
        val pearson_coefficient = pearson_dict((user_id_1, user_id_2))
        breakable {
          if (pearson_coefficient < 0.0) {
            break
          } else {
            denominator += math.abs(pearson_coefficient)
            numerator += ((
              user_movie_rating_dict((user_id_2, movie_id)) - get_user_movie_average(user_id_2, movie_id)) * pearson_coefficient)
          }
        }
        //continue to be added
      }

      var user_average = 0.0
      if(denominator == 0.0 || (numerator/denominator) < 0.0) {
        user_average = user_averages(user_id_1)
      } else {
        user_average = user_averages(user_id_1) + (numerator/denominator)
      }

      if(user_average < 0.0) {
        0.0
      } else if(user_average > 5.0) {
        5.0
      } else {
        user_average
      }

    } else {
      user_averages.getOrElse(user_id_1, 3.5)
    }


  }

  def pearson_def(x: ListBuffer[Double], y: ListBuffer[Double]): Double = {

    assert(x.size == y.size)
    val n = x.size
    assert(n > 0)
    val avg_x = average(x.toList)
    val avg_y = average(y.toList)
    var diffprod = 0.0
    var xdiff2 = 0.0
    var ydiff2 = 0.0

    for (idx <- Range(0, n)) {
      val xdiff = x(idx) - avg_x
      val ydiff = y(idx) - avg_y
      diffprod += xdiff * ydiff
      xdiff2 += xdiff * xdiff
      ydiff2 += ydiff * ydiff
    }

    if ((xdiff2 * ydiff2) != 0.0) {
      diffprod / math.sqrt(xdiff2 * ydiff2)
    } else {
      0.0
    }
  }

  def pearson(user_id_1: Int, user_id_2: Int): Double = {
    val default_value = 0.0
    val user_1_movies = user_movie_ids_dict(user_id_1)
    val user_2_movies = user_movie_ids_dict(user_id_2)

    if (!user_averages.contains(user_id_1)) {
      user_averages(user_id_1) = average(
        get_user_movie_ratings(user_id_1, user_1_movies)
      )
    }

    if(!user_averages.contains(user_id_2)) {
      user_averages(user_id_2) = average(
        get_user_movie_ratings(user_id_2, user_2_movies)
      )
    }

    var movie_1_ratings = ListBuffer[Double]()
    var movie_2_ratings = ListBuffer[Double]()


    val co_rated_movies = (user_1_movies.intersect(user_2_movies)).toList

    for (movie <- co_rated_movies) {
      movie_1_ratings += user_movie_rating_dict((user_id_1, movie))
      movie_2_ratings += user_movie_rating_dict((user_id_2, movie))
    }


    if(movie_1_ratings.size > 1){
      pearson_def(movie_1_ratings, movie_2_ratings)
    } else {
      default_value
    }

  }

  def compute_pearson_dict(user_id_1: Int, movie_id: Int): Unit = {
    if (movie_user_ids_dict.contains(movie_id)) {
      val user_ids = movie_user_ids_dict(movie_id) -- Set(user_id_1)

      for (user_id_2 <- user_ids) {
        if (!pearson_dict.contains((user_id_1, user_id_2))) {
          val corr = pearson(user_id_1, user_id_2)
          pearson_dict((user_id_1, user_id_2))= corr
          pearson_dict((user_id_2, user_id_1)) = corr
        }
      }
    }
  }

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
    for ((user_id, movie_id)<- testing_ids) {
      compute_pearson_dict(user_id, movie_id)
    }

    val data_dict = collection.mutable
      .Map(data.collect().toSeq : _*)

    val writer = new PrintWriter(new File(output_file))
    val last_index = testing_ids.size - 1

    for (((user_id, movie_id), index)<- testing_ids.zipWithIndex) {
      val rating = compute_predictions(
        user_id, movie_id)
      predictions((user_id, movie_id))= rating
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


  }
}
