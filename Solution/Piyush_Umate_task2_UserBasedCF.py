from pyspark.mllib.stat import Statistics
from pyspark import SparkConf, SparkContext
import sys
import math
import time
from itertools import combinations
from pyspark.mllib.stat import Statistics
t = time.time()
file_path = sys.argv[1]
test_file = sys.argv[2]

USER_ID = 0
MOVIE_ID = 1
RATING_ID = 2
output_file = "Piyush_Umate_UserBasedCF.txt"
spark_context = SparkContext(appName='Jaccard', conf=SparkConf())
spark_context.setLogLevel("WARN")


def update_movies(user_movie_map, movie_ids, movie_ids_count):
    movies = [-1 for x in range(movie_ids_count)]
    for movie, rating in user_movie_map[1]:
        movies[movie_ids[movie]] = rating
    return user_movie_map[0], movies


def generate_user_movie_rating_matrix(data, movie_ids, movie_ids_count):
    return data.map(lambda ((user_id,movie_id), rating): (user_id,(movie_id, rating)))\
        .groupByKey() \
        .map(
        lambda user_movie_map: update_movies(user_movie_map, movie_ids, movie_ids_count)
    ).persist()


def user_movie_map(line):
    l = line.split(',')
    if len(l) == 2:
        return (int(l[0]), int(l[1])), None
    return (int(l[USER_ID]),int(l[MOVIE_ID])), float(l[RATING_ID])


def compute_pearson(user_id_1, movie_id):
    if movie_id in movie_user_ids_dict:
        user_ids = list(movie_user_ids_dict[movie_id] - set([user_id_1]))


        for user_id_2 in user_ids:
            if not (user_id_1, user_id_2) in pearson_dict:
                corr = pearson(user_id_1, user_id_2)
                pearson_dict[(user_id_1, user_id_2)] = corr
                pearson_dict[(user_id_2, user_id_1)] = corr

def get_user_movie_average(user_id, movie_id):
    return float(average(
        get_user_movie_ratings(user_id ,user_movie_ids_dict[user_id] - set([movie_id]))
    ))

def sort_user_by_similarity(user_id_1, user_ids):
    user_similarity = {user_id_2:pearson_dict[(user_id_1, user_id_2)] for user_id_2 in user_ids}
    return [key[0] for key in sorted(user_similarity.iteritems(), key=lambda (k, v): (v, k), reverse=True)]

def compute_predictions(user_id_1, movie_id, N):
    if movie_id in movie_user_ids_dict:
        user_ids = list(movie_user_ids_dict[movie_id] - set([user_id_1]))
        user_ids = sort_user_by_similarity(user_id_1, user_ids)
        denominator, numerator = 0.0, 0.0

        for user_id_2 in user_ids[:N]:
            pearson_coefficient = pearson_dict[(user_id_1, user_id_2)]
            if pearson_coefficient < 0.0:
                continue
            denominator += abs(pearson_coefficient)
            numerator += (
                    (user_movie_rating_dict[(user_id_2, movie_id)] - get_user_movie_average(user_id_2, movie_id)) * pearson_coefficient)


        if denominator == 0 or (numerator/denominator) < 0:
            user_average = user_averages[user_id_1]
        else:
            user_average = user_averages[user_id_1] + (numerator/denominator)
        if user_average < 0.0:
            return 0.0
        elif user_average > 5.0:
            return 5.0
        return user_average

def average(x):
    assert len(x) > 0
    return float(sum(x)) / len(x)

def pearson_def(x, y):
    assert len(x) == len(y)
    n = len(x)
    assert n > 0
    avg_x = average(x)
    avg_y = average(y)
    diffprod = 0
    xdiff2 = 0
    ydiff2 = 0
    for idx in range(n):
        xdiff = x[idx] - avg_x
        ydiff = y[idx] - avg_y
        diffprod += xdiff * ydiff
        xdiff2 += xdiff * xdiff
        ydiff2 += ydiff * ydiff

    if (xdiff2 * ydiff2) != 0.0:
        return diffprod / math.sqrt(xdiff2 * ydiff2)
    else:
        return 0.0

def get_user_movie_ratings(user_id, movies):
    return [user_movie_rating_dict[(user_id, movie)] for movie in movies]

def pearson(user_id_1, user_id_2):
    default_value = 0
    user_1_movies = user_movie_ids_dict[user_id_1]
    user_2_movies = user_movie_ids_dict[user_id_2]


    #reusable module need to move out
    if user_id_1 not in user_averages:
        user_averages[user_id_1] = average(
            get_user_movie_ratings(user_id_1, user_1_movies)
        )
    if user_id_2 not in user_averages:
        user_averages[user_id_2] = average(
            get_user_movie_ratings(user_id_2, user_2_movies)
        )

    co_rated_movies = list(user_1_movies.intersection(user_2_movies))
    movie_1_ratings, movie_2_ratings = [], []
    for movie in co_rated_movies:
        movie_1_ratings.append(user_movie_rating_dict[(user_id_1,movie)])
        movie_2_ratings.append(user_movie_rating_dict[(user_id_2,movie)])


    if len(movie_1_ratings) > 1:
        return pearson_def(movie_1_ratings, movie_2_ratings)
        # movie_1_rdd = spark_context.parallelize(movie_1_ratings)
        # movie_2_rdd = spark_context.parallelize(movie_2_ratings)
        # Statistics.corr(movie_1_rdd, movie_2_rdd, method="pearson")
    return default_value



test_file = spark_context.textFile(test_file)
# test_file = test_file.coalesce(4)
test_file_header = test_file.first()
testing_data = test_file \
    .filter(lambda line: line != test_file_header) \
    .map(lambda line: user_movie_map(line))\
    .persist()

data = spark_context.textFile(file_path)
header = data.first()
data = data \
    .filter(lambda line: line != header) \
    .map(lambda line: user_movie_map(line)) \
    .persist()

training_data = data.subtractByKey(testing_data).persist()

user_movie_rating_dict = dict(training_data.collect())

#
# user_ids = training_data.keys()\
#     .keys().distinct()\
#     .persist()
#
# movie_ids = training_data.keys()\
#     .values().distinct()\
#     .persist()
#
# user_ids_count = user_ids.count()
#
# movie_ids_count = movie_ids.count()
#
# movie_ids = {
#     value: index for index, value in enumerate(movie_ids.collect())
# }
#
#
# training_data = training_data\
#     .map(lambda ((user_id,movie_id), rating): (user_id,(movie_id, rating)))\
#     .persist()
#
# user_movie_rating_matrix = generate_user_movie_rating_matrix(
#     data, movie_ids, movie_ids_count
# ).collect()



user_movie_ids = training_data\
    .map(lambda ((user_id,movie_id), rating): (user_id, movie_id))\
    .groupByKey()\
    .mapValues(set)\
    .persist()

movie_user_ids = training_data\
    .map(lambda ((user_id, movie_id), rating): (movie_id, user_id))\
    .groupByKey()\
    .mapValues(set)\
    .persist()

user_ids = user_movie_ids.keys()
movie_user_ids_dict = dict(movie_user_ids.collect())

user_movie_ids_dict = dict(user_movie_ids.collect())
pearson_dict = {}
user_averages = {}
predictions = {}
#
# user_id_combinations = combinations(user_movie_ids_dict.keys(), 2)
#
#print len(list(user_id_combinations))
#
# for user_id_1, user_id_2 in user_id_combinations:

testing_ids = testing_data.keys().collect()
for user_id, movie_id in testing_ids:
    compute_pearson(user_id, movie_id)

N = 10
file = open(output_file, 'w')
for user_id, movie_id in testing_ids:
    rating = compute_predictions(user_id, movie_id, N)
    if rating is None:
        rating = user_averages[user_id]
    file_content = ", ".join(map(str,[user_id, movie_id, rating])) + '\n'
    file.write(file_content)
    predictions[(user_id, movie_id)] = rating



data_dict = dict(data.collect())
# predictions = {key:value for key,value in predictions.iteritems() if value is not None}




abs_difference = {key: (value-data_dict[key])**2 for key,value in predictions.iteritems()}

# for key, value in predictions.iteritems():
#     if key[0] == 200 and key[1] == 78637:
#         print "predicted val", value
#         print "actual val", data_dict[key]
#
#     if abs(value-data_dict[key]) > 5:
#         print "key,val", key, value
#         print "predicted val", value
#         print "actual val", data_dict[key]

# print abs_difference
n = len(abs_difference)

#
count_dict = [0,0,0,0,0]

for k,v in predictions.iteritems():
    value = abs(v - data_dict[k])
    if value >= 0.0 and value < 1.0:
        count_dict[0] += 1
    elif value >=1.0 and value < 2.0:
        count_dict[1] += 1
    elif value >= 2.0 and value < 3.0:
        count_dict[2] += 1
    elif value >=3.0 and value < 4.0:
        count_dict[3] += 1
    else:
        count_dict[4] += 1


for index,count in enumerate(count_dict):
    if index == 0:
        print(">=0 and <1: "+ str(count))
    elif index == 1:
        print(">=1 and <2: "+ str(count))
    elif index == 2:
        print(">=2 and <3: " + str(count))
    elif index == 3:
        print(">=3 and <4: "+ str(count))
    else:
        print(">=4: "+str(count))

RMSE = math.sqrt(sum(abs_difference.values())/n)


# print n
print("RMSE: " + str(RMSE))
print("Time: %s sec" % str(time.time() - t))
# print pearson_def([4.0,5.0,5.0],[2.0,3.0,5.0])