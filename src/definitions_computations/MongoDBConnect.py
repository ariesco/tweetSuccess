from pymongo import MongoClient

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
        self.collection = collectionName

    def setData (self, dbName, collectionName):
        self.db = dbName
        self.collection = collectionName

    def reloadConection (self, url, port):
        self.client = MongoClient(url, port)
