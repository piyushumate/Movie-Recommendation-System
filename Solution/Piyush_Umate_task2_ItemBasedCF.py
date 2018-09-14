from pyspark import SparkContext, SparkConf
import sys, math
#import time
USER_ID = 0
MOVIE_ID = 1
RATING_ID = 2
import  time
t = time.time()
output_file = "Piyush_Umate_ItemBasedCF.txt"
def compute_average(x):
    return sum(x)/len(x)

def user_movie_map(line):
    l = line.split(',')
    if len(l) == 2:
        return (int(l[0]), int(l[1])), None
    return (int(l[USER_ID]),int(l[MOVIE_ID])), float(l[RATING_ID])

def sort_movie_by_similarity(movie_id_1, movie_ids):
    movie_similarity = {movie_id_2: similarity_data_dict.get((movie_id_1, movie_id_2),0.5) for movie_id_2 in movie_ids}
    #print movie_similarity
    return [key[0] for key in sorted(movie_similarity.iteritems(), key=lambda (k, v): (v, k), reverse=True)]

def compute_predictions(user_id_1, movie_id_1, N):
    movie_ids = list(user_movie_ids_dict[user_id_1] - set([movie_id_1]))
    #movie_ids = sort_movie_by_similarity(movie_id_1, movie_ids)
    numerator, denominator  = 0.0, 0.0
    for movie_id_2 in movie_ids:
        #movieid1< movieid2
        if movie_id_1 < movie_id_2:
            if (movie_id_1, movie_id_2) in similarity_data_dict:
                w = similarity_data_dict[(movie_id_1, movie_id_2)]
            else:
                continue
        else:
            if (movie_id_2, movie_id_1) in similarity_data_dict:
                w = similarity_data_dict[(movie_id_2, movie_id_1)]
            else:
                continue
        denominator += abs(w)
        numerator += (user_movie_rating_dict[(user_id_1, movie_id_2)] * w)

    if denominator == 0:
        return movie_rating_dict.get(movie_id_1, 3.5)

    rating = numerator/denominator
    if rating < 0.0:
        return 0.0
    elif rating > 5.0:
        return 5.0
    return rating


spark_context = SparkContext(appName='ItemBasedCF', conf=SparkConf())
spark_context.setLogLevel("WARN")

file_path = sys.argv[1]
test_file = sys.argv[2]
similarity_file = sys.argv[3]

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

similarity_data = spark_context.textFile(similarity_file)
similarity_data = similarity_data \
    .map(lambda line: user_movie_map(line)) \
    .persist()

training_data = data.subtractByKey(testing_data).persist()

user_movie_rating_dict = dict(training_data.collect())
similarity_data_dict = dict(similarity_data.collect())

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

movie_rating_dict = dict(training_data.map(
    lambda((user_id, movie_id), rating): (movie_id, rating))\
    .groupByKey().map(lambda (k,v): (k,compute_average(v))).collect())

user_ids = user_movie_ids.keys()
movie_user_ids_dict = dict(movie_user_ids.collect())

user_movie_ids_dict = dict(user_movie_ids.collect())

testing_ids = testing_data.keys().collect()
N = 10
predictions_dict = {}
file = open(output_file, 'w')
for user_id, movie_id in testing_ids:
    rating = compute_predictions(user_id, movie_id, N)
    file_content = ", ".join(map(str,[user_id, movie_id, rating])) + '\n'
    file.write(file_content)
    predictions_dict[(user_id, movie_id)] = rating


data_dict = dict(data.collect())
abs_difference = {key: (value-data_dict[key])**2 for key,value in predictions_dict.iteritems()}



count_dict = [0,0,0,0,0]

for k,v in predictions_dict.iteritems():
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

n = len(abs_difference)
RMSE = math.sqrt(sum(abs_difference.values())/n)


# print n
print("RMSE: " + str(RMSE))
print("Time: %s sec" % str(time.time() - t))
# print pearson_def([4.0,5.0,5.0],[2.0,3.0,5.0])
