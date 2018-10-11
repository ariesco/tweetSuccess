#  -*- coding: utf-8 -*-
import sys
import datetime
from addAbsoluteEfficiency import addAbsoluteEfficiency
from addVisibilityGraph import addVisibilityGraph
from fillClearTweet import *
from normalize import *

if __name__ == "__main__":
    print ("Start: {0}".format(datetime.datetime.now()))
   
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('Huelga') 
    db = MongoDB(connectMongoDB)
    graph = Neo4jDB (Connect2Neo4J (CONST_NEO4J_URI, CONST_NEO4J_USER, CONST_NEO4J_PASSWORD))

    """tweets = db.find(MongoDB.TWEETS_COLLECTION)
    #Inside of MongoDB.find() --> 0 = MongoDB.TWEETS_COLLECTION
    #tweet = db.find(0, getTweet("id_str",'917946195410128897'))



    process = ProcessTweet (db, graph)
    for t in tweets:
        process.process(t)"""

    #fillClearTweet(db, graph)
    addAbsoluteEfficiency(db)
    addVisibilityGraph(db, graph)
    normalize(db)


    graph.connect2Neo4J.closeDB()

    print ("\nStop: {0}".format(datetime.datetime.now()))