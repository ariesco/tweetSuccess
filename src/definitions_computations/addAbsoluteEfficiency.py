#  -*- coding: utf-8 -*-
import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *

def addAbsoluteEfficiency (dbMongo):
    print ("\nStart RSA: {0}".format(datetime.datetime.now()))

    tweets = dbMongo.find(MongoDB.CLEAR_TWEETS_COLLECTION, {CONST_RATIO_SUCCESS_ABSOLUTE: { "$exists" : False}})

    #bulk = connectMongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).initialize_unordered_bulk_op()
    bulkUpdateMap = dict()
    for t in tweets:
        visibilityValue = t[CONST_VISIBILITY_VALUE]
        rtCount = t[ProcessTweet.CONST_RT_COUNT]
        replyCount = t[ProcessTweet.CONST_REPLY_COUNT]
        quoteCount = t[ProcessTweet.CONST_QUOTE_COUNT]

        if visibilityValue <= 0:
            visibilityValue = 1

        valueRT = ProcessTweet.CONST_RT_VALUE * (rtCount / visibilityValue)
        valueReply = ProcessTweet.CONST_REPLY_VALUE * (replyCount / visibilityValue)
        valueQuote = ProcessTweet.CONST_QUOTE_VALUE * (quoteCount / visibilityValue)

        bulkUpdateMap[t[CONST_TWEET_ID]] = {CONST_RATIO_SUCCESS_ABSOLUTE: valueRT + valueReply + valueQuote}
        #bulk.find({CONST_TWEET_ID:t[CONST_TWEET_ID]}).update({"$set":{CONST_RATIO_SUCCESS_ABSOLUTE: valueRT + valueReply + valueQuote}})

    #bulk.execute()
    dbMongo.update_bulk(MongoDB.CLEAR_TWEETS_COLLECTION, bulkUpdateMap)

    print ("Stop RSA: {0}".format(datetime.datetime.now()))