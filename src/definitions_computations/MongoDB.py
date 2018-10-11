class MongoDB ():
    TWEETS_COLLECTION = 'tweets' # 0
    CLEAR_TWEETS_COLLECTION = 'clearTweet' # 1
    HISTORY_COLLECTION = "history" # 2

    def __init__(self, connect2MongoDB):
        self.connect2MongoDB = connect2MongoDB

    def find (self, collection, query = None):
        if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
            return self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).find(query)

        elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
            return self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).find(query)

        elif collection == 2 or collection == MongoDB.HISTORY_COLLECTION:
            return self.connect2MongoDB.getCollection(MongoDB.HISTORY_COLLECTION).find(query)

        return None

    def getSort (self, collection, querySort, limitSize=0):
        if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
            return self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).find().sort([querySort]).limit(limitSize)

        elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
            return self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).find().sort([querySort]).limit(limitSize)

        elif collection == 2 or collection == MongoDB.HISTORY_COLLECTION:
            return self.connect2MongoDB.getCollection(MongoDB.HISTORY_COLLECTION).find().sort([querySort]).limit(limitSize)

        return None

    def getSortV2 (self, collection, metric, querySort, limitSize=0):
        if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
            return self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).find({metric: { "$exists" : True}}).sort([querySort]).limit(limitSize)

        elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
            return self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).find({metric: { "$exists" : True}}).sort([querySort]).limit(limitSize)

        elif collection == 2 or collection == MongoDB.HISTORY_COLLECTION:
            return self.connect2MongoDB.getCollection(MongoDB.HISTORY_COLLECTION).find({metric: { "$exists" : True}}).sort([querySort]).limit(limitSize)

        return None

    def insert_one (self, collection, data = None):
        if data is not None:
            if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
                self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).insert_one(data)
                return True

            elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
                self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).insert_one(data)
                return True

            elif collection == 2 or collection == MongoDB.HISTORY_COLLECTION:
                self.connect2MongoDB.getCollection(MongoDB.HISTORY_COLLECTION).insert_one(data)
                return True
            else:
                return False
        
        else:
            return False

    def update_one (self, collection, idlabel, idvalue, data):
        if data is not None and idvalue is not None and idlabel is not None:
            if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
                self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).update_one (
                    {idlabel : idvalue},
                    {"$set": data},
                    upsert = False
                )
                return True

            elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
                self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).update_one (
                    {idlabel : idvalue},
                    {"$set": data},
                    upsert = False
                )
                return True

            elif collection == 2 or collection == MongoDB.HISTORY_COLLECTION:
                self.connect2MongoDB.getCollection(MongoDB.HISTORY_COLLECTION).update_one (
                    {idlabel : idvalue},
                    {"$set": data},
                    upsert = False
                )
                return True
            else:
                return False
        
        else:
            return False

    def update_oneV2 (self, collection, idlabel, idvalue, data):
        if data is not None and idvalue is not None and idlabel is not None:
            if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
                self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).update_one (
                    {idlabel : idvalue},
                    data,
                    upsert = False
                )
                return True

            elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
                self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).update_one (
                    {idlabel : idvalue},
                    data,
                    upsert = False
                )
                return True

            elif collection == 2 or collection == MongoDB.HISTORY_COLLECTION:
                self.connect2MongoDB.getCollection(MongoDB.HISTORY_COLLECTION).update_one (
                    {idlabel : idvalue},
                    data,
                    upsert = False
                )
                return True
            else:
                return False
        
        else:
            return False

    def update_bulk(self, collection, dataMap):
        if dataMap == None or len(dataMap) == 0:
            return False
        
        else:
            if (collection == 0 or collection == MongoDB.TWEETS_COLLECTION):
                bulk =  self.connect2MongoDB.getCollection(MongoDB.TWEETS_COLLECTION).initialize_unordered_bulk_op()

            elif collection == 1 or collection == MongoDB.CLEAR_TWEETS_COLLECTION:
                bulk =  self.connect2MongoDB.getCollection(MongoDB.CLEAR_TWEETS_COLLECTION).initialize_unordered_bulk_op()

            elif collection == 2 or collection == MongoDB.HISTORY_COLLECTION:
                bulk =  self.connect2MongoDB.getCollection(MongoDB.HISTORY_COLLECTION).initialize_unordered_bulk_op()
            else:
                return False
            
            for id in dataMap.keys():
                bulk.find({"_id":id}).update({"$set":dataMap[id]})
            
            bulk.execute()
            return True