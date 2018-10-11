#  -*- coding: utf-8 -*-
import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *

def getNSortedTweets (dbMongo, metric, sort, nTweets):
    result = []
    firstNTweets = dbMongo.getSortV2(MongoDB.CLEAR_TWEETS_COLLECTION, metric, (metric, sort), nTweets).limit(nTweets)
    if firstNTweets.count() > 0:
        for i in range (0, nTweets):
            result.append(firstNTweets[i])

        return result
    else:
        return None


def idInList (tweetId, tweets):
    result = False
    for t in tweets:
        if tweetId == t["tweetId"]:
            result = True
            break
    
    return result

def checkEquals (dbMongo, nTweet2Check):
    firstNTweets = getNSortedTweets (dbMongo, "AVG_M", -1, nTweet2Check)
    tweets = dbMongo.find(MongoDB.CLEAR_TWEETS_COLLECTION)
    print (len(firstNTweets))
    print (tweets.count())
    result = dict()

    if firstNTweets != None and tweets != None:
        for t in tweets:
            if idInList (t["tweetId"], firstNTweets):
                continue

            tHour = int(t["created_at"].split()[3].split(":")[0])
            for tc in firstNTweets:
                count = 0
                tcHour = int(tc["created_at"].split()[3].split(":")[0])

                if tc["user"]["verified"] == t["user"]["verified"]:
                    count += 1

                if tc["isReply"] == t["isReply"]:
                    count += 1

                if tc["isQuote"] == t["isQuote"]:
                    count += 1

                if (tc["urls"] == None and t["urls"] == None) or (tc["urls"] != None and t["urls"] != None and len(tc["urls"]) > 0 and len(t["urls"]) > 0):
                    count += 1

                if (tc["hashtags"] == None and t["hashtags"] == None) or (tc["hashtags"] != None and t["hashtags"] != None and len(tc["hashtags"]) > 0 and len(t["hashtags"]) > 0):
                    count += 1

                if (tc["mentions"] == None and t["mentions"] == None) or (tc["mentions"] != None and t["mentions"] != None and len(tc["mentions"]) > 0 and len(t["mentions"]) > 0):
                    count += 1

                if (tHour >= 7 and tHour <= 12 and tcHour >= 7 and tcHour <= 12) or ((tHour < 7 or tHour > 12) and (tcHour < 7 or tcHour > 12)):
                    count += 1
                
                if (tHour >= 13 and tHour <= 15 and tcHour >= 13 and tcHour <= 15) or ((tHour < 13 or tHour > 15) and (tcHour < 13 or tcHour > 15)):
                    count += 1

                if (tHour >= 16 and tHour <= 20 and tcHour >= 16 and tcHour <= 20) or ((tHour < 16 or tHour > 20) and (tcHour < 16 or tcHour > 20)):
                    count += 1

                if (tHour >= 21 and tcHour >= 21) or (tHour < 21 and tcHour < 21):
                    count += 1

                if abs(int (t["characters"]) - int(tc["characters"])) <= 15:
                    count += 1

                if abs(int (t["terms_count"]) - int(tc["terms_count"])) <= 3:
                    count += 1

                if count > 8:
                    if not tc["tweetId"] in result:
                        result[tc["tweetId"]] = dict()
                        result[tc["tweetId"]]["AVG_M"] = tc["AVG_M"]
                    
                    result[tc["tweetId"]][t["tweetId"]] = {"count": int(count), "AVG_M": t["AVG_M"]}

    return result



if __name__ == "__main__":
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)

    tweetMap = checkEquals(db, 20)

    for key in tweetMap.keys():
        #print (str(key) + " - " + str(tweetMap[key]["AVG_M"]) + ":") 
        count = 0
        for t in tweetMap[key]:
            if t == "AVG_M":
                continue
            count += 1
            #print ("\t" + str(t) + " - " + str(tweetMap[key][t]["AVG_M"]) + " - " + str(tweetMap[key][t]["count"]))
        
        print (str(key) + " - " + str(tweetMap[key]["AVG_M"]) + " - " + str (count))