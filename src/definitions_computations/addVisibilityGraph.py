import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *
from credentials import *
from Neo4JDBConnect import *
from Neo4JDB import *

def calculeVisibility (nodeId, nodeDict):
    if not nodeId in nodeDict:
        return {"visibility":0, "childs":0}
    else:
        value = 0
        nChilds = len(nodeDict[nodeId])
        for key in nodeDict[nodeId].keys():
            relationType = nodeDict[nodeId][key]["type"]
            if relationType == "QT":
                value += ProcessTweet.CONST_QUOTE_VALUE
            elif relationType == "RT":
                value += ProcessTweet.CONST_RT_VALUE
            elif relationType == "RP":
                value += ProcessTweet.CONST_REPLY_VALUE
            
            visibilityMap = calculeVisibility(nodeDict[nodeId][key]["id"], nodeDict)
            nodeDict[nodeId][key]["visibility"] = visibilityMap["visibility"]
            nodeDict[nodeId][key]["childs"] = visibilityMap["childs"]
            value += (visibilityMap["visibility"] / 2)
            nChilds += visibilityMap["childs"]

        return {"visibility":value, "childs":nChilds}

def addVisibilityGraph (dbMongo, dbNeo):
    print ("\nStart Visibility graph: {0}".format(datetime.datetime.now()))

    tweetMap = dict()

    data = list(dbNeo.getGraphs().records())
    for i in range(len(data)):
        item = data[i]
        rootId = item["rootId"]
        node = item["child"]
        relation = item["relation"][-1]

        parentId = relation.get("parentId")
        if not rootId in tweetMap:
            tweetMap[rootId] = {}
        
        if not parentId in tweetMap[rootId]:
            tweetMap[rootId][parentId] = {}

        if not relation.get("type") + str(node.get("id")) in tweetMap[rootId][parentId]:
            tweetMap[rootId][parentId][relation.get("type") + str(node.get("id"))] = {"id":node.get("id"), "visibility":0, "type":relation.get("type"), "childs":0}

    #dictionary.pop(key, None) --> delete key
    tweetVisibility = dict()
    bulkUpdateMongo = dict()
    for i in tweetMap.keys():
        auxMap = calculeVisibility (i, tweetMap[i])
        tweetVisibility[i] = {"id":i, "visibility":auxMap["visibility"], "childs":auxMap["childs"]}
        bulkUpdateMongo[i] = {CONST_MUFFLED_DISCUSSION:auxMap["visibility"], CONST_PURE_DISCUSSION:auxMap["childs"]}
        for j in tweetMap[i].keys():
            for k in tweetMap[i][j].keys():
                id = tweetMap[i][j][k]["id"]
                if not id in tweetVisibility:
                    tweetVisibility[id] = {"id":id, "visibility": tweetMap[i][j][k]["visibility"], "childs":tweetMap[i][j][k]["childs"]}
                    bulkUpdateMongo[id] = {CONST_MUFFLED_DISCUSSION:tweetMap[i][j][k]["visibility"], CONST_PURE_DISCUSSION:tweetMap[i][j][k]["childs"]}

    dbNeo.bulkUpdate(list(tweetVisibility.values()))

    data = list(dbNeo.getEmptyGraphs().records())
    for i in range(len(data)):
        nodeId = data[i]["nodeId"]
        bulkUpdateMongo[nodeId] = {CONST_MUFFLED_DISCUSSION:0, CONST_PURE_DISCUSSION: 0}

    dbMongo.update_bulk(MongoDB.CLEAR_TWEETS_COLLECTION, bulkUpdateMongo)

    print ("Stop Visibility graph: {0}".format(datetime.datetime.now()))


if __name__ == "__main__":
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)

    graph = Neo4jDB (Connect2Neo4J (CONST_NEO4J_URI, CONST_NEO4J_USER, CONST_NEO4J_PASSWORD))

    addVisibilityGraph(db, graph)