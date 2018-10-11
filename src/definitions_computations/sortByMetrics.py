#  -*- coding: utf-8 -*-
import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *

listRS = {914537401984458752: 0, 915291878123347968: 1, 917852857919070208: 2, 917427588909367296: 3, 917393319256383496: 4, 916957744292421632: 5, 917666827945349120: 6, 917821166416551936: 7, 917812778886451202: 8, 918138264808288257: 9}
listRSA = {914537401984458752: 0, 917427588909367296: 1, 915706266298798080: 2, 917750281055633408: 3, 914794945042579461: 4, 917666827945349120: 5, 917812778886451202: 6, 914541923410628609: 7, 915501543394349056: 8, 915291878123347968: 9}
listVV = {918050834398707712: 0, 917816426848772096: 1, 918066160494698496: 2, 918065431939829760: 3, 917815140598386688: 4, 918190228795478016: 5, 918068723566727169: 6, 918056519819780097: 7, 917810689389015040: 8, 917811817275158529: 9}
listMD = {918152825397497857: 0, 918165914914304003: 1, 918169282093121539: 2, 918133272084959232: 3, 918078494000721922: 4, 917879954712465414: 5, 917783890231791617: 6, 918056753287311360: 7, 918084964180201472: 8, 917779918699802625: 9}
listPD = {918152825397497857: 0, 918165914914304003: 1, 918169282093121539: 2, 918133272084959232: 3, 918078494000721922: 4, 917879954712465414: 5, 917335939478949888: 6, 917783890231791617: 7, 918084964180201472: 8, 918056753287311360: 9}

listMetric = ["RS", "RSA", "visibility_value", "MD", "PD"]

connectMongoDB = Connect2MongoDB('localhost', 27017)
connectMongoDB.setDB('test1') 
db = MongoDB(connectMongoDB)

data = dict()

print ("\nStart SORT METRIC: {0}".format(datetime.datetime.now()))

for key in listMetric:
    tweets = db.getSort(MongoDB.CLEAR_TWEETS_COLLECTION, (key,-1))
    count = 0
    for t in tweets:
        tweetId = t["tweetId"]
        if tweetId in listRS and key != "RS":
            if "RS" in data:
                if tweetId in data["RS"]:
                    data["RS"][tweetId] += "\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)
                else:
                   data["RS"][tweetId] = "tweetId:" + str(tweetId) + ",\tRS:" + str(t["RS"]) + ",\tposRS:" + str(listRS[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count) 
            else:
                data["RS"] = dict()
                data["RS"][tweetId] = "tweetId:" + str(tweetId) + ",\tRS:" + str(t["RS"]) + ",\tposRS:" + str(listRS[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)

        if tweetId in listRSA and key != "RSA":
            if "RSA" in data:
                if tweetId in data["RSA"]:
                    data["RSA"][tweetId] += "\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)
                else:
                    data["RSA"][tweetId] = "tweetId:" + str(tweetId) + ",\tRSA:" + str(t["RSA"]) + ",\tposRSA:" + str(listRSA[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)
            else:
                data["RSA"] = dict()
                data["RSA"][tweetId] = "tweetId:" + str(tweetId) + ",\tRSA:" + str(t["RSA"]) + ",\tposRSA:" + str(listRSA[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)

        if tweetId in listMD and key != "MD":
            if "MD" in data:
                if tweetId in data["MD"]:
                    data["MD"][tweetId] += "\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)
                else:
                    data["MD"][tweetId] = "tweetId:" + str(tweetId) + ",\tMD:" + str(t["MD"]) + ",\tposMD:" + str(listMD[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)
            else:
                data["MD"] = dict()
                data["MD"][tweetId] = "tweetId:" + str(tweetId) + ",\tMD:" + str(t["MD"]) + ",\tposMD:" + str(listMD[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)

        if tweetId in listPD and key != "PD":
            if "PD" in data:
                if tweetId in data["PD"]:
                    data["PD"][tweetId] += "\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)
                else:
                    data["PD"][tweetId] = "tweetId:" + str(tweetId) + ",\tPD:" + str(t["PD"]) + ",\tposPD:" + str(listPD[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count) 
            else:
                data["PD"] = dict()
                data["PD"][tweetId] = "tweetId:" + str(tweetId) + ",\tPD:" + str(t["PD"]) + ",\tposPD:" + str(listPD[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count) 

        if tweetId in listVV and key != "visibility_value":
            if "visibility_value" in data:
                if tweetId in data["visibility_value"]:
                    data["visibility_value"][tweetId] += "\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)
                else:
                    data["visibility_value"][tweetId] = "tweetId:" + str(tweetId) + ",\tvisibility_value:" + str(t["visibility_value"]) + ",\tposvisibility_value:" + str(listVV[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)
            else:
                data["visibility_value"] = dict()
                data["visibility_value"][tweetId] = "tweetId:" + str(tweetId) + ",\tvisibility_value:" + str(t["visibility_value"]) + ",\tposvisibility_value:" + str(listVV[tweetId]) + ",\t" + key + ":" + str(t[key]) + ",\tpos" + key + ":" + str(count)
        
        count += 1

    
with open("sorted.txt", 'a') as out:
    for key in data.keys():
        out.write(key + ":\n")
        for key2 in data[key].keys():
            out.write("{" + data[key][key2] + "}\n")
        out.write("\n\n")

print ("Stop SORT METRIC: {0}".format(datetime.datetime.now()))