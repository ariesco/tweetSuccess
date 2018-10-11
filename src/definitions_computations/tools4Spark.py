import datetime
import numpy


CONST_MIN_VALUE_SUCCESS = 0.000053632738945161846  #20%

def getCharacters (attribute):
    return 0 if int(attribute) <= 0 else int(attribute)

def getFollowers (attribute):
    return int (attribute)

def getVerified (attribute):
    return 1 if attribute else 0

def getMedia (attribute):
    return 1 if attribute != None and len(attribute) > 0 else 0

def getHashtags (attribute):
    return 1 if attribute != None and len(attribute) > 0 else 0

def getMentions (attribute):
    return 1 if attribute != None and len(attribute) > 0 else 0

def getCoordinates (attribute):
    try:
        return 1 if attribute != None and attribute != "" else 0
    except:
        return 1
    

def getRP (attribute):
    return 1 if attribute else 0

def getQT (attribute):
    return 1 if attribute else 0

def getTimeCol (attribute):
    return getTime(attribute.split()[3])

def getMorningCol (attribute):
    hour = int(attribute.split()[3].split(":")[0])
    if hour >= 7 and hour <= 12:
        return 1
    else:
        return 0

def getMiddayCol (attribute):
    hour = int(attribute.split()[3].split(":")[0])
    if hour >= 13 and hour <= 15:
        return 1
    else:
        return 0

def getAfternoonCol (attribute):
    hour = int(attribute.split()[3].split(":")[0])
    if hour >= 16 and hour <= 20:
        return 1
    else:
        return 0

def getNightCol (attribute):
    hour = int(attribute.split()[3].split(":")[0])
    if hour >= 21:
        return 1
    else:
        return 0

def getLabel (attribute):
    return 1 if attribute >= CONST_MIN_VALUE_SUCCESS else 0



def getFeatures (*attributes):
    """
    attributes parameters:
        * 0 --> Characters
        * 1 --> followers_count
        * 2 --> verified
        * 3 --> urls
        * 4 --> hahstags
        * 5 --> mentions
        * 6 --> coordinates
        * 7 --> isReply
        * 8 --> isQuote
        * 9 --> dateTime as String
    """
    features = []   

    features.append(0 if int(attributes[0]) <= 0 else int(attributes[0]))
    features.append(int (attributes[1]))
    features.append(1 if attributes[2] else 0)
    features.append(1 if attributes[3] != None and len(attributes[3]) > 0 else 0)
    features.append(1 if attributes[4] != None and len(attributes[4]) > 0 else 0)
    features.append(1 if attributes[5] != None and len(attributes[5]) > 0 else 0)
    features.append(1 if attributes[6] != None and attributes[6] != "" else 0)
    features.append(1 if attributes[7] else 0)
    features.append(1 if attributes[8] else 0)
    
    date = attributes[9].split()
    #features.append(getDayYear(date[5], date[1], date[2]))
    #features.append(getNumberWeekDay(date[0]))
    features.append(getTime(date[3]))

    return numpy.array(features)

def getNumberWeekDay (day):
    return {
        'Mon'      :   1,
        'Tue'      :   2,
        'Wed'      :   3,
        'Thu'      :   4,
        'Fri'      :   5,
        'Sat'      :   6,
        'Sun'      :   7
    }[day]

def getTime (hour):
    time = hour.split(":")
    return int (int(time[0]) * 60 + int(time[1]))


def getNumberMonth (month):
    return {
        'Jan'      :   "01",
        'Feb'      :   "02",
        'Mar'      :   "03",
        'Apr'      :   "04",
        'May'      :   "05",
        'Jun'      :   "06",
        'Jul'      :   "07",
        'Aug'      :   "08",
        'Sept'     :   "09",
        'Oct'      :   "10",
        'Nov'      :   "11",
        'Dec'      :   "12"
    }[month]

def getDayYear (year, month, day):
    date = datetime.datetime.strptime(year + getNumberMonth(month) + day , format='%Y%m%d')
    new_year_day = datetime.datetime(year=date.year, month=1, day=1)
    return (date - new_year_day).days + 1