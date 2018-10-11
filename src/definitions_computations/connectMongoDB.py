#  -*- coding: utf-8 -*-
import re
import sys
from pymongo import MongoClient
import pprint
from MongoDB import MongoDB

CONST_RT = "retweeted_status"
CONST_QUOTE = "quoted_status"

class Connect2MongoDB ():

    def __init__(self, url, port):
        self.client = MongoClient(url, port)
        self.db = None
        self.collection = None

    def getDB (self, *args):
        if len(args) == 0 and self.db == None:
            return None

        elif  len(args) > 0:
            return self.client[args[0]]

        return None

    def getCollection (self, *args):
        if self.db == None:
            return None

        if len(args) == 0 and self.collection == None:
            return None

        elif len(args) > 0:
            return self.client[self.db][args[0]]

        return None

    def setDB (self, dbName):
        self.db = dbName

    def setCollection (self, collectionName):
        self.collection = collection

    def setData (self, dbName, collectionName):
        self.db = dbName
        self.collection = collectionName

    def reloadConection (self, url, port):
        self.client = MongoClient(url, port)

cliente = MongoClient('localhost', 27017)
#db = cliente['test1']
clearTweet = cliente['test1'].clearTweet

mongoDB = Connect2MongoDB('localhost', 27017)
mongoDB.setDB('test1') 

def getTweet (id_str):
    cursor = (mongoDB.getCollection('tweets')
                .find(
                    {"id_str": id_str}
                )
            )
    return cursor

def getAllTweet ():
    return mongoDB.getCollection('tweets').find()

def getRTs ():
    return (mongoDB.getCollection('tweets')
                .find(
                    {"retweeted_status": { "$exists" : True}}
                )
            )

def getQuotes ():
    return (mongoDB.getCollection('tweets')
                .find(
                    {"quoted_status": { "$exists" : True}}
                )
            )

def getOriginals ():
    return (mongoDB.getCollection('tweets')
                .find(
                    {"$and": [
                        {"quoted_status": { "$exists" : False}},
                        {"retweeted_status": { "$exists" : False}}
                    ]}
                )
            )

def getReplys ():
    return (mongoDB.getCollection('tweets')
                .find(
                    {"in_reply_to_status_id_str": {"$ne": None}}
                )
            )

def processRT(tweet):
    originalTweet = getOriginalTweet(tweet[CONST_RT])

    if clearTweet.find({"tweetId": originalTweet["id_str"]}).count() == 0:
        insertTweet(originalTweet)

    
    
def getOriginalTweet(tweet):
    if CONST_QUOTE in tweet:
        return getOriginalTweet(tweet[CONST_QUOTE])
    else:
        return tweet

def processQuote(tweet, isRT):
    pass

def insertTweet (tweet):
    pass

"""
tweet = getTweet('917946195410128897')
print(sys.getdefaultencoding())
print (tweet.count())
for t in tweet:
    print (t["text"])
"""
db = MongoDB(mongoDB)
RTcount = 0
quoteCount = 0
tweets = db.find(MongoDB.TWEETS_COLLECTION) #getAllTweet()
rts = db.find(MongoDB.TWEETS_COLLECTION, {"retweeted_status": { "$exists" : True}}) #getRTs()
quotes = getQuotes()
originals = getOriginals()
replys = getReplys()

for t in tweets:
    isRT = False
    if CONST_RT in t:
        isRT = True
        RTcount += 1
        processRT(t)
    
    if CONST_QUOTE in t:
        quoteCount += 1
        processQuote(t, isRT)

print ("{0} RTs of {1} tweets".format(RTcount, tweets.count()))
print ("{0} quotes of {1} tweets".format(quoteCount, tweets.count()))
print ("{0} original tweets".format(tweets.count() - RTcount - quoteCount))
print ("{0} RTs".format(rts.count()))
print ("{0} Quotes".format(quotes.count()))
print ("{0} Originals".format(originals.count()))
print ("{0} replys".format(replys.count()))