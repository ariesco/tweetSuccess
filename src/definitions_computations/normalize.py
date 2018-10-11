#  -*- coding: utf-8 -*-
import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *

CONST_AVG_METRIC = "AVG_M"

def getFirstSorted (dbMongo, metric, sort):
    tweets = dbMongo.getSortV2(MongoDB.CLEAR_TWEETS_COLLECTION, metric, (metric, sort), 1)
    if tweets.count() > 0:
        return tweets[0][metric]
    else:
        return None

def getMax (dbMongo, metric):
    return getFirstSorted (dbMongo, metric, -1)

def getMin (dbMongo, metric):
    return getFirstSorted (dbMongo, metric, 1)

def getWeight (metric):
    if metric == CONST_RATIO_SUCCESS:
        return 1

    elif metric == CONST_RATIO_SUCCESS_ABSOLUTE:
        return 1

    elif metric == CONST_VISIBILITY_VALUE:
        return 0.4

    elif metric == CONST_MUFFLED_DISCUSSION:
        return 1

    elif metric == CONST_PURE_DISCUSSION:
        return 1

    else:
        return 0

def normalize (dbMongo):
    print ("\nStart Normalize: {0}".format(datetime.datetime.now()))
    listMetric = [CONST_RATIO_SUCCESS, CONST_RATIO_SUCCESS_ABSOLUTE, CONST_VISIBILITY_VALUE, CONST_MUFFLED_DISCUSSION, CONST_PURE_DISCUSSION]
    weights = dict()
    normalizedSuffix = "_normalized"
    maxMins = dict()

    for metric in listMetric:
        max = getMax (dbMongo, metric)
        min = getMin (dbMongo, metric)
        maxMins[metric] = {"max":max, "min":min}
        weights[metric + normalizedSuffix] = getWeight(metric)

    tweets = dbMongo.find(MongoDB.CLEAR_TWEETS_COLLECTION)
    bulkUpdateMap = dict()
    for t in tweets:  
        for metric in listMetric:
            if metric in t:
                valueNormalized = (t[metric] - maxMins[metric]["min"]) / (maxMins[metric]["max"] - maxMins[metric]["min"])
                
                if not t[CONST_TWEET_ID] in bulkUpdateMap:
                    bulkUpdateMap[t[CONST_TWEET_ID]] = dict()

                bulkUpdateMap[t[CONST_TWEET_ID]][metric + normalizedSuffix] = valueNormalized
        
        newMetric = 0
        totalWeight = 0
        for key in bulkUpdateMap[t[CONST_TWEET_ID]].keys():
            newMetric += weights[key] * bulkUpdateMap[t[CONST_TWEET_ID]][key]
            totalWeight += weights[key]
        
        newMetric /= totalWeight
        bulkUpdateMap[t[CONST_TWEET_ID]][CONST_AVG_METRIC] = newMetric

    dbMongo.update_bulk(MongoDB.CLEAR_TWEETS_COLLECTION, bulkUpdateMap)
    print ("Stop Normalize: {0}".format(datetime.datetime.now()))


if __name__ == "__main__":
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)

    normalize(db)
