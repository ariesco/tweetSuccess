from queries import *
from inserts import *
from updates import *

class ProcessTweet ():
    CONST_RT = "retweeted_status"
    CONST_QUOTE = "quoted_status"
    CONST_REPLY = "in_reply_to_status_id"
    CONST_ID = "id"
    CONST_TRUNCATED = "truncated"
    CONST_FULL_TEXT = "full_text"
    CONST_TEXT = "text"
    CONST_EXTENDED_TWEET = "extended_tweet"
    CONST_REPLY_TO_STATUS_ID = "in_reply_to_status_id"
    CONST_QUOTE_TO_STATUS_ID = "quoted_status_id"
    CONST_QUOTE_STATUS = "is_quote_status"
    CONST_ENTITIES = "entities"
    CONST_DISPLAY_TEXT_RANGE = "display_text_range"
    CONST_HASHTAGS = "hashtags"
    CONST_USER_MENTIONS = "user_mentions"
    CONST_SYMBOLS = "symbols"
    CONST_MEDIA = "media"
    CONST_REPLY_COUNT = "reply_count"
    CONST_RT_COUNT = "retweet_count"
    CONST_QUOTE_COUNT = "quote_count"
    CONST_FAVORITE_COUNT = "favorite_count"
    CONST_LANGUAGE = "lang"
    CONST_COORDINATES = "coordinates"
    CONST_USER = "user"
    CONST_FOLLOWERS_COUNT = "followers_count"
    CONST_CREATED_AT = "created_at"
    CONST_URLS = "urls"

    CONST_RT_VALUE = 1
    CONST_QUOTE_VALUE = 1
    CONST_REPLY_VALUE = 0.5
    
    def __init__ (self, dataBase, graph):
        self.db = dataBase
        self.graph = graph
        self.nodes2Update = {}
       
    def defineType (self, tweet):
        if ProcessTweet.CONST_RT in tweet:
            self.isRT = True
        else:
            self.isRT = False

        if ProcessTweet.CONST_QUOTE in tweet:
            self.isQuote = True
        else:
            self.isQuote = False

        if tweet[ProcessTweet.CONST_REPLY] != None:
            self.isReply = True
        else:
            self.isReply = False


    def processNormalTweet (self, tweet, isReply, isQuote):
        tweetId = tweet[ProcessTweet.CONST_ID]
        clearTweets = self.db.find(1, getTweet ("_id", tweetId))
        if clearTweets.count() == 0:
            self.db.insert_one(1, getInsertClearTweet(tweet, isQuote, isReply, self))
        else:
            print ("update process")
            updateFields = []
            print ("Saved RT {0} -- New RT {1}".format(clearTweets[0][ProcessTweet.CONST_RT_COUNT], tweet[ProcessTweet.CONST_RT_COUNT]))
            if clearTweets[0][ProcessTweet.CONST_RT_COUNT] < tweet[ProcessTweet.CONST_RT_COUNT]:
                updateFields.append (ProcessTweet.CONST_RT_COUNT)
            
            print ("Saved FAV {0} -- New FAV {1}".format(clearTweets[0][ProcessTweet.CONST_FAVORITE_COUNT], tweet[ProcessTweet.CONST_FAVORITE_COUNT]))
            if clearTweets[0][ProcessTweet.CONST_FAVORITE_COUNT] < tweet[ProcessTweet.CONST_FAVORITE_COUNT]:
                updateFields.append (ProcessTweet.CONST_FAVORITE_COUNT)

            print ("Saved QT {0} -- New QT {1}".format(clearTweets[0][ProcessTweet.CONST_QUOTE_COUNT], tweet[ProcessTweet.CONST_QUOTE_COUNT]))
            if clearTweets[0][ProcessTweet.CONST_QUOTE_COUNT] < tweet[ProcessTweet.CONST_QUOTE_COUNT]:
                updateFields.append (ProcessTweet.CONST_QUOTE_COUNT)

            if len(updateFields) > 0:
                print("update done")
                self.db.update_one(1, "_id", tweetId, updateClearTweets(tweet, updateFields))
            

    def process (self, tweet):
        auxTweet = tweet
        tweets = []
        if ProcessTweet.CONST_RT in tweet:
            tweets.append({"type":"RT", "tweet":auxTweet, "isReply":False})
            auxTweet = auxTweet[ProcessTweet.CONST_RT]
            

        while ProcessTweet.CONST_QUOTE in auxTweet and auxTweet[ProcessTweet.CONST_QUOTE_STATUS] is True:
            isReply = auxTweet[ProcessTweet.CONST_REPLY] != None
            tweets.append({"type":"quote", "tweet":auxTweet, "isReply":isReply})
            #self.processNormalTweet(auxTweet, isReply, True)
            auxTweet = auxTweet[ProcessTweet.CONST_QUOTE]

        isReply = auxTweet[ProcessTweet.CONST_REPLY] != None
        tweets.append({"type":"normal", "tweet":auxTweet, "isReply":isReply})
        #self.processNormalTweet(auxTweet, isReply, False)

        for t in reversed(tweets):
            self.auxProcess(t)

        if len(self.nodes2Update) > 0:
            self.graph.bulkUpdate(list(self.nodes2Update.values()))
            self.nodes2Update.clear()
    
    def auxProcess (self, tweetInfo):
        if tweetInfo["type"] == "RT":
            self.processRT (tweetInfo["tweet"])
        elif tweetInfo["type"] == "quote":
            self.processQuote(tweetInfo["tweet"], tweetInfo["isReply"])
        else:
            self.processNormal(tweetInfo["tweet"], tweetInfo["isReply"], False, None)

    def processRT (self, tweet):
        toTweetId = tweet[ProcessTweet.CONST_RT][ProcessTweet.CONST_ID]
        tweetId = tweet[ProcessTweet.CONST_ID]
        userId = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_ID]
        date = tweet[ProcessTweet.CONST_CREATED_AT]

        if self.exist(tweet[ProcessTweet.CONST_ID], userId, toTweetId, 'RT', date) == False:
            updateFields=[]
            if toTweetId != None:
                value = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_RT_VALUE
                updateFields.append({"field":CONST_VISIBILITY_VALUE, "value":value, "type":"+"})
                updateFields.append({"field":CONST_VISIBILITY_COUNT_RT, "value":1, "type":"+"})
                self.db.update_oneV2(1, CONST_TWEET_ID, toTweetId, updateClearTweetsV2(updateFields))
                self.graph.upsertTypeAndRelation(toTweetId, tweetId, "RT")
                #self.updateParentGraph(tweetId, ProcessTweet.CONST_RT_VALUE)


    def exist (self, tweetId, userId, toTweetId, type_str, date, insert=True):
        #Change this query to search history. It could be wrong, it should search by userId, toTweetId and type_str
        #Then if it exists, it should check tweetId. 
        history = self.db.find(2, getHistoryV2(toTweetId,userId, type_str))
        if history.count() == 0:
            if insert:
                self.db.insert_one(2, getInsertHistory(tweetId, userId, toTweetId, type_str, date))
            return False
        else:
            return True

    def processQuote (self, tweet, isReply):
        tweetId = tweet[ProcessTweet.CONST_ID]
        userId = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_ID]
        date = tweet[ProcessTweet.CONST_CREATED_AT]
        quoteId = tweet[ProcessTweet.CONST_QUOTE_TO_STATUS_ID]
        value2Quote =  ProcessTweet.CONST_QUOTE_VALUE
        replyId = None
        updateFieldsQuote=[]
        updateFieldsReply=[]
        if isReply:
            replyId = tweet[ProcessTweet.CONST_REPLY_TO_STATUS_ID]
            #self.exist(tweetId, userId, quoteId, "RP", date)

        #if self.exist(tweetId, userId, quoteId, "QT", date) == False:
        clearTweets = self.db.find(1, getTweet ("_id", tweetId))
        if clearTweets.count() == 0:
            self.db.insert_one(2, getInsertHistory(tweetId, userId, quoteId, "QT", date))

            if quoteId != None:
                self.graph.upsertTypeAndRelation(quoteId, tweetId, "QT")
                #self.updateParentGraph(tweetId, ProcessTweet.CONST_QUOTE_VALUE)

            if quoteId != None and replyId != None and quoteId != replyId:
                value = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_REPLY_VALUE
                updateFieldsReply.append({"field":CONST_VISIBILITY_VALUE, "value":value, "type":"+"})
                updateFieldsReply.append({"field":CONST_VISIBILITY_COUNT_REPLY, "value":1, "type":"+"})

                value2 = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_QUOTE_VALUE
                updateFieldsQuote.append({"field":CONST_VISIBILITY_VALUE, "value":value2, "type":"+"})
                updateFieldsQuote.append({"field":CONST_VISIBILITY_COUNT_QUOTE, "value":1, "type":"+"})

                self.db.update_oneV2(1, CONST_TWEET_ID, quoteId, updateClearTweetsV2(updateFieldsQuote))
                self.db.update_oneV2(1, CONST_TWEET_ID, replyId, updateClearTweetsV2(updateFieldsReply))
                #print ("QT: {0}\n RP: {1}".format(quoteId, replyId))
                #TODO: Change to bulk

            elif quoteId != None and replyId != None and quoteId == replyId:
                if ProcessTweet.CONST_QUOTE_VALUE > ProcessTweet.CONST_REPLY_VALUE:
                    value = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_QUOTE_VALUE
                else:
                    value = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_REPLY_VALUE

                updateFieldsQuote.append({"field":CONST_VISIBILITY_VALUE, "value":value, "type":"+"})
                updateFieldsQuote.append({"field":CONST_VISIBILITY_COUNT_QUOTE, "value":1, "type":"+"})
                updateFieldsQuote.append({"field":CONST_VISIBILITY_COUNT_REPLY, "value":1, "type":"+"})
                self.db.update_oneV2(1, CONST_TWEET_ID, quoteId, updateClearTweetsV2(updateFieldsQuote))
                #print("QT and RT same toTweetId {0}".format(quoteId))

            elif quoteId != None and replyId == None:
                value = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT] * ProcessTweet.CONST_QUOTE_VALUE
                updateFieldsQuote.append({"field":CONST_VISIBILITY_VALUE, "value":value, "type":"+"})
                updateFieldsQuote.append({"field":CONST_VISIBILITY_COUNT_QUOTE, "value":1, "type":"+"})
                self.db.update_oneV2(1, CONST_TWEET_ID, quoteId, updateClearTweetsV2(updateFieldsQuote))

            self.processNormal(tweet, isReply, True, None)

        else:
            self.processNormal(tweet, isReply, True, clearTweets)
       

    def processNormal (self, tweet, isReply, isQuote, clearTweets):
        tweetId = tweet[ProcessTweet.CONST_ID]
        userId = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_ID]

        if clearTweets == None and isQuote == False:
            clearTweets = self.db.find(1, getTweet ("_id", tweetId))

        if (isQuote == True and clearTweets == None) or (clearTweets.count() == 0 and isQuote == False):
            self.db.insert_one(1, getInsertClearTweet(tweet, isQuote, isReply, self))
            if isReply:
                self.db.insert_one(2, getInsertHistory(tweetId, userId, tweet[ProcessTweet.CONST_REPLY_TO_STATUS_ID], "RP", tweet[ProcessTweet.CONST_CREATED_AT]))
                self.insertNormalReplyNodeGraph (tweetId, tweet[ProcessTweet.CONST_REPLY_TO_STATUS_ID])
            else:
                if isQuote == False:
                    self.graph.upsert(True, tweetId)
        else:
            #print ("update process")
            updateFields = []
            #print ("Saved RT {0} -- New RT {1}".format(clearTweets[0][ProcessTweet.CONST_RT_COUNT], tweet[ProcessTweet.CONST_RT_COUNT]))
            if clearTweets[0][ProcessTweet.CONST_RT_COUNT] < tweet[ProcessTweet.CONST_RT_COUNT]:
                updateFields.append (ProcessTweet.CONST_RT_COUNT)
            
            #print ("Saved FAV {0} -- New FAV {1}".format(clearTweets[0][ProcessTweet.CONST_FAVORITE_COUNT], tweet[ProcessTweet.CONST_FAVORITE_COUNT]))
            if clearTweets[0][ProcessTweet.CONST_FAVORITE_COUNT] < tweet[ProcessTweet.CONST_FAVORITE_COUNT]:
                updateFields.append (ProcessTweet.CONST_FAVORITE_COUNT)

            #print ("Saved QT {0} -- New QT {1}".format(clearTweets[0][ProcessTweet.CONST_QUOTE_COUNT], tweet[ProcessTweet.CONST_QUOTE_COUNT]))
            if clearTweets[0][ProcessTweet.CONST_QUOTE_COUNT] < tweet[ProcessTweet.CONST_QUOTE_COUNT]:
                updateFields.append (ProcessTweet.CONST_QUOTE_COUNT)

            if len(updateFields) > 0:
                #print("update done")
                dictionary = updateClearTweets(tweet, updateFields)
                dictionary[CONST_RATIO_SUCCESS] = self.getRatioSuccess(tweet)
                self.db.update_one(1, "_id", tweetId, dictionary)
        


    def getRatioSuccess (self, tweet):
        followersCount = tweet[ProcessTweet.CONST_USER][ProcessTweet.CONST_FOLLOWERS_COUNT]
        rtCount = tweet[ProcessTweet.CONST_RT_COUNT]
        replyCount = tweet[ProcessTweet.CONST_REPLY_COUNT]
        quoteCount = tweet[ProcessTweet.CONST_QUOTE_COUNT]

        if followersCount <= 0:
            followersCount = 1

        valueRT = ProcessTweet.CONST_RT_VALUE * (rtCount / followersCount)
        valueReply = ProcessTweet.CONST_REPLY_VALUE * (replyCount / followersCount)
        valueQuote = ProcessTweet.CONST_QUOTE_VALUE * (quoteCount / followersCount)

        return valueRT + valueReply + valueQuote

    def getOriginalTweet(self, tweet):
        auxTweet = tweet
        while ProcessTweet.CONST_QUOTE in auxTweet:
            auxTweet = auxTweet[ProcessTweet.CONST_QUOTE]
        
        return auxTweet

    def updateParentGraph (self, sourceId, valueToAdd):
        data = list(self.graph.searchTweet(sourceId).records())
        if (len(data) > 0):
            source = data[0]["tweet"]
            level = source.get("level")
            data = list(self.graph.getPath(sourceId).records())
            size = len(data)
            for i in range(size):
                node = data[i]["tweet"]
                if not node.get("id") in self.nodes2Update:
                    self.nodes2Update[node.get("id")] = {"id":node.get("id"), "visibility":0}
            
                #print ("Node {0} -- level {1}".format(node.get("id"), node.get("level")))
                self.nodes2Update[node.get("id")]["visibility"] += (valueToAdd / (level - node.get("level")))

    def insertNormalReplyNodeGraph (self, tweetId, replyId):
        clearTweets = self.db.find(1, getTweet ("_id", replyId))
        if clearTweets.count() == 0:
            self.graph.upsert(True, replyId)
        
        self.graph.upsertTypeAndRelation(replyId, tweetId, "RP")
                

    