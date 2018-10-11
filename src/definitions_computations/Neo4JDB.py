class Neo4jDB():

    def __init__(self, connect2Neo4J):
        self.connect2Neo4J = connect2Neo4J
        self.attempts = 3

    def callDB (self, query):
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query)
        else:
            return None


    def getGraphs (self):
        query = """
            Match (n:Huelga {root:True})<-[r:RELTYPE *..]-(n2:Huelga) 
            RETURN n.id AS rootId , n2 AS child , r AS relation
        """
        db = self.connect2Neo4J.getDB()
        countAttempts = 0
        if db != None:
            while countAttempts < self.attempts:
                try:
                    return db.run(query)
                except Exception as e:
                    countAttempts+=1
                    print("Exception {0}: {1} {2}. Not critical. Continuing...".format(str(countAttempts), type(e), e))
            
            if countAttempts >= self.attempts:
                print("GetGraphs can't be executed.")
                return None
        else:
            return None

    def getEmptyGraphs (self):
        query = """
            Match (n:Huelga {root:True}) WHERE NOT (n)<-[:RELTYPE *..]-(:Huelga) 
            RETURN n.id as nodeId
        """
        db = self.connect2Neo4J.getDB()
        countAttempts = 0
        if db != None:
            while countAttempts < self.attempts:
                try:
                    return db.run(query)
                except Exception as e:
                    countAttempts+=1
                    print("Exception {0}: {1} {2}. Not critical. Continuing...".format(str(countAttempts), type(e), e))
            
            if countAttempts >= self.attempts:
                print("GetGraphs can't be executed.")
                return None
        else:
            return None

    def upsert (self, root, tweetId):
        query = "MERGE (t:Huelga {id:$id}) ON CREATE SET t = {id:$id, root:$root, visibility:0, childs:0}"
        db = self.connect2Neo4J.getDB()
        countAttempts = 0
        if db != None:
            while countAttempts < self.attempts:
                try:
                    return db.run(query, id=tweetId, root=root)
                except Exception as e:
                    countAttempts+=1
                    print("Exception {0}: {1} {2}. Not critical. Continuing...".format(str(countAttempts), type(e), e))
            
            if countAttempts >= self.attempts:
                print("Upsert can't be executed. TweetId: {0}".format(tweetId))
                return None
        else:
            return None

    def upsertTweet (self, tweetId, value):
        """
        query = "MERGE (t:Huelga {value: '" + tweetId + "'}) "
        query += "ON MATCH SET t.visibility = t.visibility + " + str(value) + " "
        query += "ON CREATE SET t = {value: '" + tweetId + "', visibility: 0} "
        """
        query =     "MERGE (t:Huelga {id:$id}) " 
        query +=    "ON MATCH SET t.visibility = t.visibility + $visivility " 
        query +=    "ON CREATE SET t = {id:$id, visibility:0}"
        
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=tweetId, visivility=value)
        else:
            return None

    def upsertTypeAndRelation (self,parentId, tweetId, reltype):
        query = """
            MATCH (t2: Huelga {id:$parent})
            USING INDEX t2:Huelga(id) 
            MERGE (t:Huelga {id:$id})
            ON CREATE SET t = {id:$id, root:$boolean, visibility:0, childs:0}
            ON MATCH SET t.root = $boolean
            MERGE (t2)<-[r:RELTYPE]-(t)
            ON CREATE SET r = {type:$relType, parentId:$parent}
            ON MATCH SET r = {type:$relType, parentId:$parent}
        """

        db = self.connect2Neo4J.getDB()
        countAttempts = 0
        if db != None:
            while countAttempts < self.attempts:
                try:
                    return db.run(query, id=tweetId, parent = parentId, relType = reltype, boolean = False)
                except Exception as e:
                    countAttempts+=1
                    print("Exception {0}: {1} {2}. Not critical. Continuing...".format(str(countAttempts), type(e), e))
            
            if countAttempts >= self.attempts:
                print("UpsertTypeAndRelation can't be executed. TweetId: {0}".format(tweetId))
                return None
        else:
            return None


    def searchTweet (self, tweetId):
        query = "MATCH (t:Huelga {id:$id}) RETURN t as tweet"
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=tweetId)
        else:
            return None

    def getPath (self, sourceId):
        query = "MATCH (t1:Huelga {id:$id})-[r:RELTYPE *..]->(t2:Huelga) RETURN t2 as tweet ORDER BY t2.level DESC"
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=sourceId)
        else:
            return None

    def getChilds (self, sourceId):
        query = "MATCH (t1:Huelga {id:$id})<-[r:RELTYPE *..]-(t2:Huelga) RETURN t2 as tweet ORDER BY t2.level ASC"
        db = self.connect2Neo4J.getDB()
        if db != None:
            return db.run(query, id=sourceId)
        else:
            return None

    def insertTweet (self, dataToInsert):
        query = "CREATE (t:Huelga $data)"
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, data=dataToInsert)
            return True
        else:
            return False

    def updateHead (self, parentId, tweetId, relType):
        query = """
        MATCH (t:Huelga {id:$id})
        USING INDEX t:Huelga(id) 
        SET t.type = $relType, t.level = 0, t.parentId = $parentId
        """
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, id=tweetId, relType=relType, parentId=parentId)
            return True
        else:
            return False

    def insertRelation (self, parentId, childId):
        query = "MATCH (t1:Huelga {id:$id1}), (t2:Huelga {id:$id2}) CREATE (t1)-[r:RELTYPE]->(t2)"
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, id1=childId, id2=parentId)
            return True
        else:
            return False
    
    def insertTweetAndRelation (self, parentId, childId, relType):
        query = "MATCH (t1:Huelga {id:$parentId}) CREATE (t2:Huelga {id:$childId, visibility:0, parentId:t1.id})-[r:RELTYPE {type:$relType}]->(t1)"
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, parentId=parentId, childId=childId, relType=relType)
            return True
        else:
            return False

    def bulkUpdate (self, dataList):
        query = """
            UNWIND {rows} as row
            MATCH (t:Huelga {id:row.id}) 
            USING INDEX t:Huelga(id) 
            SET t.visibility = row.visibility, t.childs = row.childs
            """
        countAttempts = 0
        db = self.connect2Neo4J.getDB()
        if db != None:
            while countAttempts < self.attempts:
                try:
                    db.run(query, rows=dataList)
                    return True
                except Exception as e:
                    countAttempts+=1
                    print("Exception {0}: {1} {1}. Not critical. Continuing...".format(str(countAttempts), type(e), e))
            
            if countAttempts >= self.attempts:
                print("BulkUpdate can't be executed. Datalist: {0}".format(dataList))
                return False
        else:
            return False
    
    def addOneLevel (self, sourceId):
        query = """
            MATCH (t:Huelga {id:$id})<-[r:RELTYPE *..]-(t2:Huelga)
            USING INDEX t:Huelga(id) 
            SET t2.level = t2.level + 1
        """
        db = self.connect2Neo4J.getDB()
        if db != None:
            db.run(query, id=sourceId)
            return True
        else:
            return False
            