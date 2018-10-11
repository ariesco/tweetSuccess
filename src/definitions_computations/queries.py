def getRTs ():
    return {"retweeted_status": { "$exists" : True}}

def getQuotes ():
    return {"quoted_status": { "$exists" : True}}

def getNormalTweets():
    return ({
            "$and": [
                        {"quoted_status": { "$exists" : False}},
                        {"retweeted_status": { "$exists" : False}},
                        {"in_reply_to_status_id_str": None}
                    ]
            })

def getReplys ():
    return {"in_reply_to_status_id": {"$ne": None}}

def getTweet (idLabel, idValue):
    return {idLabel: idValue}

def getHistory(tweetId, userId = None, toTweetId=None, type_str = None):
    query = {}
    query ["tweetId"] = tweetId
    if userId != None:
        query ["userId"] = userId
    if toTweetId != None:
        query["toTweetId"] = toTweetId
    if type_str != None:
        query["type"] = type_str

    return query

def getHistoryV2(toTweetId, userId = None, type_str = None):
    query = {}
    query["toTweetId"] = toTweetId
    if userId != None:
        query ["userId"] = userId
    if type_str != None:
        query["type"] = type_str

    return query




