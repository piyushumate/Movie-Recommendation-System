from pyspark import SparkConf, SparkContext
import sys, time
from operator import add
import itertools

USER_ID = 0
MOVIE_ID = 1

t = time.time()
hash_functions = 8
bands = 4
a = 7
b = 3
mod_value = 671

spark_context = SparkContext(appName='Jaccard', conf=SparkConf())
spark_context.setLogLevel("WARN")
similarity_threshold = 0.5

output_file = "Piyush_Umate_SimilarMovie_Jaccard.txt"


# def get_movies(index1, index2):
#     return reverse_movie_ids[index1], reverse_movie_ids[index2]
#
#
def generate_signature(characteristic_matrix, row_count, column_count, hash_functions):
    permutations_array = [
        [0 for j in range(hash_functions)] for i in range(row_count)
    ]

    # hash functions * movies
    signature_matrix = [
        [sys.maxint for j in range(column_count)] for i in range(hash_functions)
    ]

    for row_index in range(row_count):
        for hash_function_factor in range(hash_functions):
            permutations_array[row_index][hash_function_factor] = \
                ((a * row_index) + b * (hash_function_factor + 1)) % mod_value

        for column in range(column_count):
            if characteristic_matrix[row_index][MOVIE_ID][column]:
                for hash_function_factor in range(hash_functions):
                    signature_matrix[hash_function_factor][column] = min(
                        permutations_array[row_index][hash_function_factor],
                        signature_matrix[hash_function_factor][column]
                    )

    # matrix transpose

    return signature_matrix


#
#

def update_movies(user_movie_map, movie_ids, movie_ids_count):
    movies = [0 for x in range(movie_ids_count)]
    for movie in user_movie_map[1]:
        movies[movie_ids[movie]] = 1
    return user_movie_map[0], movies


#
#
def user_movie_map(line):
    l = line.split(',')
    return int(l[USER_ID]), int(l[MOVIE_ID])


#
#
def read_input():
    file_path = sys.argv[1]

    ratings = spark_context.textFile(file_path)
    # ratings file header
    header = ratings.first()

    # ratings = ratings.coalesce(coalesce_factor)

    ratings = ratings.filter(lambda line: line != header) \
        .map(lambda line: user_movie_map(line)) \
        .persist()

    return ratings


def generate_characteristic_matrix(ratings, movie_ids, movie_ids_count):
    return ratings.groupByKey() \
        .map(
        lambda user_movie_map: update_movies(user_movie_map, movie_ids, movie_ids_count)
    ).persist()


ratings = read_input()
user_ids = ratings.keys().distinct().persist()

movie_ids = ratings.values().distinct().persist()
movie_ids_count = movie_ids.count()

movie_ids = {value: index for index, value in enumerate(movie_ids.collect())}

characteristic_matrix = generate_characteristic_matrix(
    ratings, movie_ids, movie_ids_count
).collect()

#
#
user_ids_count = user_ids.count()
signature_matrix = generate_signature(
    characteristic_matrix,
    user_ids_count,
    movie_ids_count,
    hash_functions
)


#
#
def similar_movies(chunk):
    chunk = list(chunk)
    chunk = [chunk_part for key, chunk_part in chunk]
    chunk_transpose = map(list, zip(*chunk))
    chunk_transpose_length = len(chunk_transpose)
    for index1 in range(chunk_transpose_length):
        for index2 in range(index1 + 1, chunk_transpose_length):
            if chunk_transpose[index1] == chunk_transpose[index2]:
                yield (index1, index2)


#
def jaccard_similarity(candidate_pair_chunk):
    for index1, index2 in candidate_pair_chunk:
        list1 = characteristic_matrix[index1]
        list2 = characteristic_matrix[index2]
        # similarity = float(len(set1.intersection(set2)))/float(len(set1.union(set2)))
        sum_list = list(map(add, list1, list2))
        one_count = sum_list.count(1)
        two_count = sum_list.count(2)
        similarity = float(two_count) / float(two_count + one_count)
        if similarity >= similarity_threshold:
            movie_1 = reverse_movie_ids[index1]
            movie_2 = reverse_movie_ids[index2]
            if movie_1 < movie_2:
                yield ((movie_1, movie_2), similarity)
            if movie_1 > movie_2:
                yield ((movie_2, movie_1), similarity)


#
#
def signature_matrix_with_key(signature_matrix):
    key_signature_matrix = []
    for index, row in enumerate(signature_matrix):
        key_signature_matrix.append((index + 1, row))
    return key_signature_matrix


#
signature_matrix_rdd = spark_context.parallelize(signature_matrix_with_key(signature_matrix))
# #print signature_matrix.getNumPartitions()
signature_matrix_rdd = signature_matrix_rdd.partitionBy(bands)
candidate_pairs = signature_matrix_rdd.mapPartitions(lambda chunk: similar_movies(chunk)) \
    .distinct()
#
# #signature_matrix = map(set, zip(*signature_matrix))
#
characteristic_matrix = [x for user_id, x in characteristic_matrix]
characteristic_matrix = map(list, zip(*characteristic_matrix))
#
reverse_movie_ids = {v: k for k, v in movie_ids.iteritems()}
#

candidate_pairs = candidate_pairs.coalesce(1)
movies_similar = candidate_pairs.mapPartitions(
    lambda candidate_pair_chunk: jaccard_similarity(candidate_pair_chunk)
).sortByKey(True).persist()


def toCSVLine(k, v):
    return str(k[0]) + ', ' + str(k[1]) + ', ' + str(v)


lines = movies_similar.map(lambda (k, v): toCSVLine(k, v)).collect()

file = open(output_file, 'w')
file_content = '\n'.join(lines)
file.write(file_content)

print "Time: %s sec" % (time.time() - t)
#
# def test(block):
#     yield len(list(block))
#
#
# rdd = spark_context.parallelize([(0, [i, i+1]) for i in range(1,101)])

# rdd = rdd.partitionBy(100)
# print rdd.mapPartitions(lambda block: test(block)).collect()
