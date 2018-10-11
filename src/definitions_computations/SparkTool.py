from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from tools4Spark import *

class SparkTool ():

    CONST_SEED = 5
    CONST_TYPE_REGRESSION = "models"
    CONST_TYPE_CLASIFICATION = "clasification"
    

    def __init__ (self, typeSparkTool, dbName="test1", collectionName="clearTweet"):
        self.typeSparkTool = typeSparkTool
        self.db = dbName
        self.collection = collectionName
        self.splitDF = None

        self.initSpark()
        self.createDBConnection()
        self.initUdfFunction()
        self.transformDF()

    def initSpark (self):
        self.spark = SparkSession \
        .builder \
        .appName("myAppTesting") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/" + self.db + "." + self.collection) \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + self.db + "." + self.collection) \
        .getOrCreate()

    def createDBConnection (self):
        self.df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",
            "mongodb://127.0.0.1/" + self.db + "." + self.collection).load()

    def initUdfFunction (self):
        self.getCharacters_udf = udf (getCharacters, IntegerType())
        self.getFollowers_udf = udf (getFollowers, IntegerType())
        self.getVerified_udf = udf (getVerified, IntegerType())
        self.getMedia_udf = udf (getMedia, IntegerType())
        self.getHashtags_udf = udf (getHashtags, IntegerType())
        self.getMentions_udf = udf (getMentions, IntegerType())
        self.getCoordinates_udf = udf (getCoordinates, IntegerType())
        self.getRP_udf = udf (getRP, IntegerType())
        self.getQT_udf = udf (getQT, IntegerType())
        self.getTimeCol_udf = udf (getTimeCol, IntegerType())
        self.getNightCol_udf = udf (getNightCol, IntegerType())
        self.getAfternoonCol_udf = udf (getAfternoonCol, IntegerType())
        self.getMiddayCol_udf = udf (getMiddayCol, IntegerType())
        self.getMorningCol_udf = udf (getMorningCol, IntegerType())
        self.getLabel_udf = udf (getLabel, IntegerType())

    def transformDF (self):
        self.df = self.df.drop("coordinates")
        (maxC, minC, maxT, minT, maxF, minF) = self.df.agg(F.max("characters"), F.min("characters"), F.max("terms_count"), F.min("terms_count"), F.max("user.followers_count"), F.min("user.followers_count")).head()

        self.df = self.df.withColumn("characters", (self.df.characters - minC) / (maxC - minC))
        self.df = self.df.withColumn("terms_count", (self.df.terms_count - minT) / (maxT - minT))
        self.df = self.df.withColumn("verified", self.getVerified_udf(self.df.user.verified))
        self.df = self.df.withColumn("followers", (self.df.user.followers_count - minF) / (maxF - minF))
        self.df = self.df.withColumn("media", self.getMedia_udf(self.df.urls))
        self.df = self.df.withColumn("hashtags", self.getHashtags_udf(self.df.hashtags))
        self.df = self.df.withColumn("mentions", self.getMentions_udf(self.df.mentions))
        #self.df = self.df.withColumn("coordinates", getCoordinates_udf(df.coordinates))
        self.df = self.df.withColumn("rp", self.getRP_udf(self.df.isReply))
        self.df = self.df.withColumn("qt", self.getQT_udf(self.df.isQuote))
        #self.df.withColumn("weekDay", self.df.created_at.split()[0])
        #self.df.withColumn("yearDay", )
        #self.df = self.df.withColumn("time", self.getTimeCol_udf(self.df.created_at))
        self.df = self.df.withColumn("morning", self.getMorningCol_udf(self.df.created_at))
        self.df = self.df.withColumn("midday", self.getMiddayCol_udf(self.df.created_at))
        self.df = self.df.withColumn("afternoon", self.getAfternoonCol_udf(self.df.created_at))
        self.df = self.df.withColumn("night", self.getNightCol_udf(self.df.created_at))

        if self.typeSparkTool == SparkTool.CONST_TYPE_CLASIFICATION:
            self.df = self.df.withColumn("label", self.getLabel_udf(self.df.AVG_M))
        else:
            self.df = self.df.withColumnRenamed("PD_normalized", "label")
            self.df = self.df.filter(self.df.label >= 0)

        self.df = self.df.drop("RS", "favorite_count", "visibility_value", "retweet_count", "created_at", "replyTo", "visibility_count_RT", \
                        "tweetId", "reply_count", "text", "visibility_count_reply", "isReply", "visibility_count_quote", "user", "urls", \
                        "quote_count", "quoteTo", "isLong", "isQuote", "symbols", "lang", "RSA", "PD", "MD", "MD_normalized", \
                        "AVG_M", "RS_normalized", "PD_normalized", "RSA_normalized", "visibility_value_normalized")
        
        assembler = VectorAssembler(
            inputCols=["characters", "followers", "verified", "media", "hashtags", "mentions", "rp", "qt", "terms_count", "morning", "midday", "afternoon", "night"], outputCol="features"
        )
        self.df = assembler.transform(self.df)

    def createSplitDF (self):
        if self.typeSparkTool == SparkTool.CONST_TYPE_CLASIFICATION:
            self.splitDF = []

            self.splitDF.append (self.df.stat.sampleBy("label", {0:0.6, 1:0.6}, SparkTool.CONST_SEED))
            df2 = self.df.join(self.splitDF[0], "_id", "left_anti")
            self.splitDF.append (df2.stat.sampleBy("label", {0:0.5, 1:0.5}, SparkTool.CONST_SEED))
            self.splitDF.append (df2.join(self.splitDF[1], "_id", "left_anti"))
        else:
            self.splitDF = self.df.randomSplit([0.6, 0.2, 0.2],  SparkTool.CONST_SEED)

    
    def getSpark (self):
        return self.spark

    def getFullDf (self):
        return self.df

    def getTrainDF (self):
        if self.splitDF == None:
            self.createSplitDF()

        return self.splitDF[0]

    def getMetricDF (self):
        if self.splitDF == None:
            self.createSplitDF()

        return self.splitDF[1]

    def getTestDF (self):
        if self.splitDF == None:
            self.createSplitDF()

        return self.splitDF[2]