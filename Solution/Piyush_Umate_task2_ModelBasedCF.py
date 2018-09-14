import sys
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import time,math
from operator import add

file_path = sys.argv[1]
test_file = sys.argv[2]
output_file = "Piyush_Umate_ModelBasedCF.txt"
t = time.time()
def user_movie_map(line):
    l = line.split(',')
    if len(l) == 2:
        return (int(l[0]), int(l[1])), None
    return (int(l[0]),int(l[1])), float(l[2])

def compute_range(value):
    if value >= 0.0 and value < 1.0:
        return 0
    elif value >= 1.0 and value < 2.0:
        return 1
    elif value >= 2.0 and value < 3.0:
        return 2
    elif value >= 3.0 and value < 4.0:
        return 3
    else:
        return 4

spark_context = SparkContext(appName='ModelBasedCF', conf=SparkConf())
spark_context.setLogLevel("WARN")
test_file = spark_context.textFile(test_file)
# test_file = test_file.coalesce(4)
test_file_header = test_file.first()
testing_data = test_file \
    .filter(lambda line: line != test_file_header) \
    .map(lambda line: user_movie_map(line))\
    .persist()

# print "testing data", testing_data.count()

data = spark_context.textFile(file_path)
# data = data.coalesce(4)
header = data.first()
data = data \
    .filter(lambda line: line != header) \
    .map(lambda line: user_movie_map(line)) \
    .persist()


training_data = data.subtractByKey(testing_data).map(
    lambda r: Rating(r[0][0],r[0][1],r[1])
).persist()
# print training_data.count(), "training data"
#testing_data = testing_data.keys().persist()



rank = 10
numIterations = 12

model = ALS.train(training_data, rank, numIterations, 0.1)
predictions = model.predictAll(testing_data.keys()).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds =data.map(lambda r: r).join(predictions)
RMSE = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
# print predictions.collect()n

def toCSVLine(k, v):
    return str(k[0]) + ', ' + str(k[1]) + ', ' + str(v)


lines = predictions.sortByKey().map(lambda (k, v): toCSVLine(k, v)).collect()

file = open(output_file, 'w')
file_content = '\n'.join(lines)
file.write(file_content)

baseline = ratesAndPreds.map(
        lambda r: (compute_range(abs(r[1][0]-r[1][1])),1)
    ).countByKey()\
    .items()

for (index, count) in baseline:
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

print("RMSE: " + str(RMSE))
print("Time: %s sec" % str(time.time() - t))
# model.save(spark_context, "target")


#
# print data.collect()
# print testing_data.collect()
# print training_data.count()
# print time.time() - t

# l = spark_context.parallelize([("a", 1), ("b", 1), ("a", 1)])
# print l.countByKey().items()