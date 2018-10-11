import sys
import pathlib
from functools import partial
from queueThreads import PoolThreads

CONST_SUFIX = "_Clear"
CONST_TYPE = ".json"
CONST_START_JSON = "{"
CONST_NEW_DIR = "Clear_Files"

def clearJson (file, toDir):
    fileParts = file.parts
    try:
        with open("/".join(fileParts), "r") as inputFile:
            with open(toDir + "/" + fileParts[-1] + CONST_SUFIX + CONST_TYPE, "w") as outputFile:
                for line in inputFile:
                    if line[0] == CONST_START_JSON:
                        outputFile.write(line)
    except Exception as e:
        print("Error in the file \"{0}\" {1}: {2}.".format("/".join(fileParts), type(e), e))

def searchFiles (dir):
    """
    Search and return all files in a directory
        :param dir: Directory to search files
    """
    if dir == None:
        return []

    return [file for file in pathlib.Path(dir).iterdir() if file.is_file()]

def clearFiles (fromDir, toDir=CONST_NEW_DIR, numberThreads=1):
    pool = PoolThreads (numberThreads)
    listFiles = searchFiles(fromDir)
    pathlib.Path(toDir).mkdir(parents=True, exist_ok=True)
    
    for file in listFiles:
        pool.mainQueue.put(partial(clearJson, file, toDir))

    pool.start()

if __name__ == "__main__":
    try:
        if len(sys.argv) == 2:
            clearFiles(sys.argv[1])
        elif len(sys.argv) == 3:
            clearFiles(sys.argv[1], sys.argv[2])
        elif len(sys.argv) > 3:
            clearFiles(sys.argv[1], sys.argv[2], int(sys.argv[3]))
        elif len(sys.argv) <= 0:
            print("You should pass at least one file's name")
    except Exception as e:
        print("Error {0}: {1}.".format(type(e), e))



    