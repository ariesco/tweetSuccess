#  -*- coding: utf-8 -*-
import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *
from Neo4JDBConnect import *
from Neo4JDB import *
from credentials import *

def fillClearTweet(dbMongo, dbNeo):
    print ("\nStart Fill clear tweet: {0}".format(datetime.datetime.now()))
   
    """connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)
    graph = Neo4jDB (Connect2Neo4J (CONST_NEO4J_URI, CONST_NEO4J_USER, CONST_NEO4J_PASSWORD))"""

    tweets = dbMongo.find(MongoDB.TWEETS_COLLECTION)
    #Inside of MongoDB.find() --> 0 = MongoDB.TWEETS_COLLECTION
    #tweet = db.find(0, getTweet("id_str",'917946195410128897'))



    process = ProcessTweet (dbMongo, dbNeo)
    for t in tweets:
        process.process(t)


    #graph.connect2Neo4J.closeDB()

    print ("Stop Fill clear tweet: {0}".format(datetime.datetime.now()))