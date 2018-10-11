"""
import sys
sys.path.append('D:/Windows/Mis documentos D/librerias python/mleap/python')
import mleap
"""

from pyspark.ml.regression import LinearRegression, LinearRegressionModel, GeneralizedLinearRegression, RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.ml.feature import Normalizer
from pyspark.ml.feature import MinMaxScaler
from pyspark.sql import functions as F

from pyspark.sql.types import IntegerType, ArrayType, FloatType
from pyspark.sql.functions import udf
from tools4Spark import *
from crossValidation import bestLinearReggresion, bestGeneralizedLR, bestRandomForestRegressor
from modelsSpark import ModelsSpark


#sc = SparkContext('local')
#spark = SparkSession(sc)
spark = SparkSession.builder.master("local").getOrCreate()





df = spark.createDataFrame([
    (1, 10, 31),
    (1, 10, 32),
    (1, 10, 33),
    (1, 10, 34),
    (1, 10, 35),
    (1, 10, 36),
    (1, 10, 37),
    (1, 10, 38),
    (1, 10, 39),
    (1, 10, 311),
    (1, 10, 312),
    (1, 10, 313),
    (1, 10, 314),
    (1, 10, 315),
    (1, 10, 316),
    (1, 10, 317),
    (1, 10, 318),
    (1, 10, 319),
    (1, 10, 3191),
    (1, 10, 3192),
    (1, 1, 1001),
    (0, 1, 1002),
    (0, 1, 1003),
    (0, 1, 1004),
    (0, 1, 1005),
    (0, 1, 1006),
    (0, 1, 1007),
    (0, 1, 1008),
    (0, 1, 1009),
    (0, 1, 10011),
    (0, 1, 10012),
    (0, 1, 10013),
    (0, 1, 10014),
    (0, 1, 10015),
    (0, 1, 10016),
    (0, 1, 10017),
    (0, 1, 10018),
    (0, 1, 10019),
    (0, 1, 10021),
    (0, 1, 10022),
    (0, 1, 10023),
    (0, 1, 10024),
    (0, 1, 10025),
    (0, 1, 10026),
    (0, 1, 10027),
    (0, 1, 10028),
    (0, 1, 10029),
    (0, 1, 10031),
    (0, 1, 10032),
    (0, 1, 10033),
    (0, 1, 10034),
    (0, 1, 10035),
    (0, 1, 10036),
    (0, 1, 10038),
    (0, 1, 10039),
    (0, 1, 10040),
    (0, 1, 10041),
    (0, 1, 10042),
    (0, 1, 10043),
    (0, 1, 10044),
    (0, 1, 10045),
    (0, 1, 10046),
    (0, 1, 10047),
    (0, 1, 10048),
    (0, 1, 10049)
], ["id", "a", "b"])


df.groupBy("id").count().show()

#(maxA, minA, maxB, minB) = df.agg(F.max("a"), F.min("a"), F.max("b"), F.min("b")).head()

sampled = df.stat.sampleBy("id", {0:0.6, 1:0.6})
sampled.groupBy("id").count().show()

df2 = df.join(sampled, "b", "left_anti")
sampled2 = df2.stat.sampleBy("id", {0:0.5, 1:0.5})
sampled2.groupBy("id").count().show()

sampled3 = df2.join(sampled2, "b", "left_anti")
sampled3.groupBy("id").count().show()

df3 = sampled.unionByName(sampled2).unionByName(sampled3)
df3.groupBy("id").count().show()


"""
ms = ModelsSpark()
lrModel = ms.getOrCreateLR()
glrModel = ms.getOrCreateGLR()
rfrModel = ms.getOrCreateRFR()
"""

"""
spark = SparkSession \
    .builder \
    .appName("myAppTesting") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test1.clearTweet") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test1.clearTweet") \
    .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",
"mongodb://127.0.0.1/test1.clearTweet").load()

getFeatures_udf = udf (getFeatures, ArrayType(IntegerType()))
getCharacters_udf = udf (getCharacters, IntegerType())
getFollowers_udf = udf (getFollowers, IntegerType())
getVerified_udf = udf (getVerified, IntegerType())
getMedia_udf = udf (getMedia, IntegerType())
getHashtags_udf = udf (getHashtags, IntegerType())
getMentions_udf = udf (getMentions, IntegerType())
getCoordinates_udf = udf (getCoordinates, IntegerType())
getRP_udf = udf (getRP, IntegerType())
getQT_udf = udf (getQT, IntegerType())
getTimeCol_udf = udf (getTimeCol, IntegerType())
getNightCol_udf = udf (getNightCol, IntegerType())
getAfternoonCol_udf = udf (getAfternoonCol, IntegerType())
getMiddayCol_udf = udf (getMiddayCol, IntegerType())
getMorningCol_udf = udf (getMorningCol, IntegerType())
#df = df.withColumn("coordinates", df.coordinates.cast("string"))

(maxC, minC, maxT, minT, maxF, minF) = df.agg(F.max("characters"), F.min("characters"), F.max("terms_count"), F.min("terms_count"), F.max("user.followers_count"), F.min("user.followers_count")).head()
print (maxC)
print(minC)
print(maxT)
print(minT)
print(maxF)
print(minF)

#df = df.withColumn("characters", getCharacters_udf(df.characters))
df = df.withColumn("characters", (df.characters - minC) / (maxC - minC))
df = df.withColumn("terms_count", (df.terms_count - minT) / (maxT - minT))
df = df.withColumn("followers", (df.user.followers_count - minF) / (maxF - minF))
df = df.withColumn("verified",getVerified_udf(df.user.verified))
df = df.withColumn("media", getMedia_udf(df.urls))
df = df.withColumn("hashtags", getHashtags_udf(df.hashtags))
df = df.withColumn("mentions", getMentions_udf(df.mentions))
#df = df.withColumn("coordinates", getCoordinates_udf(df.coordinates))
df = df.withColumn("rp", getRP_udf(df.isReply))
df = df.withColumn("qt", getQT_udf(df.isQuote))
#df.withColumn("weekDay", df.created_at.split()[0])
#df.withColumn("yearDay", )
#df = df.withColumn("time", getTimeCol_udf(df.created_at))
df = df.withColumn("morning", getMorningCol_udf(df.created_at))
df = df.withColumn("midday", getMiddayCol_udf(df.created_at))
df = df.withColumn("afternoon", getAfternoonCol_udf(df.created_at))
df = df.withColumn("night", getNightCol_udf(df.created_at))

df = df.withColumnRenamed("AVG_M", "label")
df = df.drop("RS", "favorite_count", "visibility_value", "retweet_count", "created_at", "replyTo", "visibility_count_RT", \
                "tweetId", "reply_count", "text", "visibility_count_reply", "isReply", "visibility_count_quote", "user", "urls", \
                "quote_count", "quoteTo", "isLong", "isQuote", "symbols", "lang", "RSA", "PD", "MD", "RSA_normalized", "MD_normalized", \
                "visibility_value_normalized", "RS_normalized", "PD_normalized", "coordinates")

df.show(20)
"""
"""
df = df.withColumn("features", getFeatures_udf(df.characters, df.user.followers_count, df.user.verified, df.urls, \
                                                df.hashtags, df.mentions, df.coordinates, df.isReply, df.isQuote, \
                                                df.created_at)) \
        .withColumnRenamed("AVG_M", "label") \
        .drop("_id", "RS", "favorite_count", "visibility_value", "retweet_count", "created_at", "replyTo", "visibility_count_RT", \
                "tweetId", "reply_count", "text", "visibility_count_reply", "isReply", "visibility_count_quote", "user", "urls", \
                "quote_count", "quoteTo", "isLong", "isQuote", "symbols", "lang", "RSA", "PD", "MD", "RSA_normalized", "MD_normalized", \
                "visibility_value_normalized", "RS_normalized", "PD_normalized", "coordinates", "media", "terms_count", "mentions", \
                "hashtags", "characters")
"""
"""
assembler = VectorAssembler(
  inputCols=["characters", "followers", "verified", "media", "hashtags", "mentions", "rp", "qt", "time"], outputCol="features"
)
df = assembler.transform(df)"""
#df.count()
#df.show(5)

#splitDF = df.randomSplit([0.2, 0.2, 0.6])

#train a model with a dataframe
#lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
#lrModel = lr.fit(splitDF[2])
"""
lrModel = bestLinearReggresion (splitDF[2], splitDF[1], "mse")
glrModel = bestGeneralizedLR (splitDF[2], splitDF[1], "mse")
rfrModel = bestRandomForestRegressor (splitDF[2], splitDF[1], "mse")
"""
#glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3, tol=0.0)
#lrModel = glr.fit(splitDF[2])

#rfr = RandomForestRegressor(numTrees=5, maxDepth=0, subsamplingRate=0.66)
#lrModel = rfr.fit(splitDF[2])

"""
lr = LinearRegression(maxIter=10, labelCol="label", featuresCol="features")
ev = RegressionEvaluator(metricName="mse")
paramGrid = ParamGridBuilder() \
    .addGrid(lr.elasticNetParam, [1.0, 0.6, 0.2]) \
    .addGrid(lr.regParam, [1.0, 0.6, 0.2]) \
    .addGrid(lr.tol, [1.0, 0.6, 0.2]) \
    .build()
pipeline = Pipeline(stages=[lr])

print ("\nStart Linear Regression train: {0}".format(datetime.datetime.now()))
lrcv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=ev,
    numFolds=3
)

lrModel = lrcv.fit(splitDF[2])
print ("\nStop Linear Regression train: {0}".format(datetime.datetime.now()))
#Load a model from a file
#lrModel = LinearRegressionModel.load("file:///D:/Data_TFM/code/models/LinearRegression")
"""


"""
predictions0 = lrModel.transform(splitDF[1])
evaluator = RegressionEvaluator(metricName="mse")
mse = evaluator.evaluate(predictions0)
evaluator.setMetricName("rmse")
rmse = evaluator.evaluate(predictions0)
evaluator.setMetricName("mae")
mae = evaluator.evaluate(predictions0)
print("Mean Squared Error: " + str(mse))
print("Root Mean Squared Error: " + str(rmse))
print("Mean absolute error: " + str(mae))


lrModel.write().overwrite().save("file:///D:/Data_TFM/code/models/LinearRegression")
glrModel.write().overwrite().save("file:///D:/Data_TFM/code/models/GeneralizedLinearRegression")
rfrModel.write().overwrite().save("file:///D:/Data_TFM/code/models/RandomForestRegressor")
"""
# Load training data
"""training = spark.read.format("libsvm")\
    .load("sparkData/sample_regression_data.txt")"""

"""

training = spark.createDataFrame(
    [
        (0, Vectors.dense([1])),
        (0, Vectors.dense([2])),
        (1, Vectors.dense([3])),
        (1, Vectors.dense([4]))
    ],
    ["label", "features"]
)

lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(training)

test0 = spark.createDataFrame(
    [
        (0, Vectors.dense([-1])),
        (0, Vectors.dense([1])),
        (1, Vectors.dense([6])),
        (1, Vectors.dense([4]))
    ],
    ["label", "features"]
)
predictions0 = lrModel.transform(test0)
print(predictions0.collect)
result = predictions0.select("features", "label", "prediction") \
    .collect()

for row in result:
    print("features=%s, label=%s -> prediction=%s"
          % (row.features, row.label, row.prediction))

"""
"""
# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)"
"""