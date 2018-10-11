from neo4j.v1 import GraphDatabase

class Connect2Neo4J ():
    
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.session = self.driver.session()
        self.started = True

    def getDB(self):
        if self.started:
            return self.session
        else:
            return None

    def closeDB (self):
        if self.session != None:
            self.session.close()
            self.started = False