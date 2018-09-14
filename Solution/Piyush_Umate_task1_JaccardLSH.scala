import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer


object JaccardLSH {

  val a = 7
  val b = 3
  val mod_value = 671
  val USER_ID = 0
  val MOVIE_ID = 1
  val bands = 10
  var characteristic_matrix_values : Array[Array[Int]]= Array()
  var r_movie_id_map =  Map[Int, Int]()
  val similarity_threshold = 0.5
  val output_file = "Piyush_Umate_SimilarMovie_Jaccard.txt"
  def user_movie_map(line: String): (Int, Int) = {
    var l = line.split(",")
    (l(USER_ID).toInt, l(MOVIE_ID).toInt)
  }

  def read_input(data: RDD[String]): RDD[(Int, Int)] = {
    val header = data.first()

    data.filter(line => line != header)
      .map(line => user_movie_map(line))
      .persist()
  }

  def update_movies(user_movie_map: (Int, Iterable[Int]), movie_ids: Map[Int, Int], count: Int): (Int, Array[Int])  = {
    var movies = Array.fill(count){0}

    for (movie <- user_movie_map._2) {
      movies(movie_ids(movie)) = 1
    }

    (user_movie_map._1, movies)
  }

  def generate_characteristic_matrix(ratings: RDD[(Int, Int)], movie_ids: Map[Int, Int], count: Int) : RDD[(Int, Array[Int])] = {
    ratings.groupByKey()
      .map(user_movie_map => update_movies(user_movie_map, movie_ids, count))
      .persist()
  }

  def generate_signature(characteristic_matrix: Array[(Int, Array[Int])], row_count: Int, column_count: Int, hash_functions: Int):Array[Array[Int]] = {
    var permutations_array = Array.fill(row_count, hash_functions)(0)
    var signature_matrix = Array.fill(hash_functions, column_count)(Int.MaxValue)

    for (row_index <- Range(0, row_count)) {
      for (hash_function_factor <- Range(0, hash_functions)) {
        permutations_array(row_index)(hash_function_factor) = ((a*row_index)+b*(hash_function_factor+1)) % mod_value
      }

      for (column <- Range(0, column_count)) {
        if (characteristic_matrix(row_index)._2(column) == 1) {
          for (hash_function_factor <- Range(0, hash_functions)) {
            signature_matrix(hash_function_factor)(column) = Math.min(
              permutations_array(row_index)(hash_function_factor),
              signature_matrix(hash_function_factor)(column)
            )
          }
        }
      }
    }

    signature_matrix

  }

  def signature_matrix_with_key(signature_matrix: Array[Array[Int]]): Array[(Int, Array[Int])] = {
    var key_signature_matrix : Array[(Int, Array[Int])] = Array()
    for ((row, index)<- signature_matrix.zipWithIndex) {
      key_signature_matrix :+= (index+1, row)
    }
    key_signature_matrix
  }


  def similar_movies(chunk: Iterator[(Int, Array[Int])]) : Iterator[(Int,Int)] =  {
    var similar_movie_indices =  ListBuffer[(Int,Int)]()
    val chunk_transpose = chunk.toArray.map(_._2).transpose

    val chunk_transpose_length = chunk_transpose.size

//    for (index1 <- Range(0, chunk_transpose_length)) {
//      for (index2 <- Range(index1+1, chunk_transpose_length)) {
//        if (chunk_transpose(index1).sameElements(chunk_transpose(index2))) {
//          similar_movie_indices = similar_movie_indices :+ (index1, index2)
//        }
//      }
//    }

    var index1 = 0
    var index2 = 0
    while(index1 < chunk_transpose_length) {
      index2 = index1 + 1
      while(index2 < chunk_transpose_length) {
        if (chunk_transpose(index1) sameElements (chunk_transpose(index2))) {
          similar_movie_indices.append((index1, index2))
        }
        index2 = index2 + 1
      }
      index1 = index1 + 1
    }
    similar_movie_indices.toIterator
  }


  def jaccard_similarity(candidate_pair_chunk: Iterator[(Int, Int)]) : Iterator[((Int, Int), Float)]= {
    var similar_movies: Array[((Int, Int), Float)] = Array()

    for((index1, index2) <- candidate_pair_chunk) {
      var list1 = characteristic_matrix_values(index1)
      var list2 = characteristic_matrix_values(index2)
      var sum_list = (list1, list2).zipped.map(_ + _)
      var one_count = sum_list.count(_ == 1)
      var two_count = sum_list.count(_ == 2)
      var similarity = (two_count.toFloat) / ((two_count+one_count).toFloat)
      if (similarity >= similarity_threshold) {
        val movie_1 = r_movie_id_map(index1)
        val movie_2 = r_movie_id_map(index2)
        if(movie_1 < movie_2) {
          similar_movies :+= ((movie_1, movie_2), similarity)
        } else {
          similar_movies :+= ((movie_2, movie_1), similarity)
        }

      }
    }
    similar_movies.toIterator
  }


  def main(args: Array[String]): Unit = {
    val t0 = System.currentTimeMillis()
    val hash_functions = 10
    val conf = new SparkConf()
    val spark_context = new SparkContext(conf)
//    spark_context.setLogLevel("WARN")
    val file_path = args(0)
    var ratings = read_input(
      spark_context.textFile(file_path)
    )

    val user_ids = ratings.keys
      .distinct().persist()


    var movie_ids = ratings.values
      .distinct().persist()

    val movie_ids_count = movie_ids.count().toInt

    var movie_ids_map = movie_ids
      .collect()
      .toArray
      .zipWithIndex
      .map{ case (value,index) => value->index }
      .toMap

    val characteristic_matrix = generate_characteristic_matrix(
      ratings, movie_ids_map, movie_ids_count
    ).collect()


    val user_ids_count = user_ids.count().toInt
    val signature_matrix = generate_signature(
      characteristic_matrix,
      user_ids_count,
      movie_ids_count,
      hash_functions
    )

//    signature_matrix.foreach(x => x.foreach(println))
//    val x = movie_ids.collect()
//
//    val characteristic_matrix = generate_characteristic_matrix(
//      ratings, movie_ids, movie_ids_count
//    )



    var signature_matrix_rdd = spark_context.parallelize(
      signature_matrix_with_key(signature_matrix)
    )

//    signature_matrix_rdd.collect().foreach(x => x._2.foreach(println))
//
    signature_matrix_rdd = signature_matrix_rdd.partitionBy(
      new HashPartitioner(bands)
    )
//
//
//

    var candidate_pairs = signature_matrix_rdd.mapPartitions(
      chunk => similar_movies(chunk)
    ).distinct().persist()



    characteristic_matrix_values = characteristic_matrix.map(_._2)

    characteristic_matrix_values = characteristic_matrix_values.transpose
//

    candidate_pairs = candidate_pairs.coalesce(1)
    r_movie_id_map = movie_ids_map.map(_.swap)

//    movie_ids_map.foreach(println)
    val movies_similar = candidate_pairs.mapPartitions(
      candidate_pair_chunk => jaccard_similarity(candidate_pair_chunk)
    ).sortByKey().persist()

//    movies_similar.collect().foreach(println)

    val similarity = movies_similar.collect()

//    similarity.map()
//    movies_similar.map(
//      similar_movies => Array(
//        similar_movies._1._1.toString(), similar_movies._1._2.toString(), similar_movies._2.toString()
//      ).mkString(", ")
//    ).saveAsTextFile(output_file)

    val writer = new PrintWriter(new File(output_file))
    val last_index = similarity.size - 1

    for ((((user_id, movie_id),rating), index) <- similarity.zipWithIndex) {

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
    println("Time: " + (System.currentTimeMillis() - t0)/1000 + "sec")
  }

}
