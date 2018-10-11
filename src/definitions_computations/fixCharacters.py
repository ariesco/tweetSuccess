import sys
import datetime
from MongoDBConnect import *
from MongoDB import *
from queries import *
from processTweet import *


def countCharacters (text, isReply):
    count = 0
    isUrl = False
    index = 0
    endReply = not isReply
    isUser = False
    for char in text:
        if not endReply:
            if isUser:
                if char == ' ':
                    isUser = False
                    if index + 1 < len(text) and text[index+1] != '@':
                        endReply = True
            else: 
                isUser = True
        else:
            if isUrl and char == ' ':
                isUrl = False
                count += 1
            elif not isUrl and char != 'h':
                count += 1
            elif not isUrl:
                if text.startswith('https://', index, index + 8) or text.startswith('http://', index, index + 7):
                    isUrl = True
                    count -= 1
                else:
                    count += 1

        index += 1
    
    count = 0 if count < 0 else count
    return count

def countTerms (text, isReply):
    words = text.split()
    count = 0
    endReply = not isReply

    for word in words:
        if not endReply and not word.startswith('@'):
            endReply = True
        
        if endReply:
            if not word.startswith('https://') and not word.startswith('http://'):
                count += 1
    
    return count



def fixCharacters (dbMongo):
    print ("\nStart fixCharacters: {0}".format(datetime.datetime.now()))

    tweets = dbMongo.find(MongoDB.CLEAR_TWEETS_COLLECTION)
    bulkUpdateMap = dict()
    for t in tweets:
        characters = countCharacters(t[ProcessTweet.CONST_TEXT], t[CONST_IS_REPLY])
        terms = countTerms(t[ProcessTweet.CONST_TEXT], t[CONST_IS_REPLY])
        #if terms <= 2 or terms > 35:
            #print ("\nTermss: " + str(terms) + "\nText:" + t[ProcessTweet.CONST_TEXT] + "\nId:" + str(t["tweetId"]) + "\n")
        #if characters > 280:
            #print ("\nCharacters: " + str(characters) + "\nText:" + t[ProcessTweet.CONST_TEXT] + "\nId:" + str(t["tweetId"]) + "\n")
        bulkUpdateMap[t[CONST_TWEET_ID]] = {CONST_CHARACTERS: characters, CONST_TERMS_COUNT: terms}

    dbMongo.update_bulk(MongoDB.CLEAR_TWEETS_COLLECTION, bulkUpdateMap)
    print ("Stop fixCharacters: {0}".format(datetime.datetime.now()))


if __name__ == "__main__":
    connectMongoDB = Connect2MongoDB('localhost', 27017)
    connectMongoDB.setDB('test1') 
    db = MongoDB(connectMongoDB)

    fixCharacters(db)


"""
text = "@Santi_ABASCAL El PSOE que tanto dinero daba a cataluña para callarles la boca y no la liaran con independencia...tanto buenismo tantas mierdas y mira..."
text2 = "Manifest del Grup de Diàleg Interreligiós d'Olot davant la situació política de Catalunya: https://t.co/z3GoZibgEc https://t.co/r90abGmkuh" #should be 114
text3 = "Parece que os preocupa más la independencia de Catalunya que echar a Rajoy. https://t.co/ykX8Knsiet" #la url no cuenta


print(countCharactersNoUrl(text3))
"""