import threading
import queue
import time
import psutil
from functools import partial

class PoolThreads ():

    def __init__(self, nThreads=1):
        self.threadCount = 0
        self.exitFlag = True
        self.mainQueue = queue.Queue(0)
        self.queueLock = threading.Lock()
        self.nThreads = nThreads
        self.threads = []
        self.active = False
        if (self.nThreads <= 0):
            self.nThreads = psutil.cpu_count() - 1
        
        self.createThreads()

    def createThreads(self):
        for i in list(range(0, self.nThreads)):
            thread = PoolThread("Thread {0}".format(self.threadCount), self.threadCount, self)
            self.threads.append(thread)
            self.threadCount += 1

    def doAction(self):
        while self.exitFlag == False:
            self.queueLock.acquire()
            if not self.mainQueue.empty():
                action = self.mainQueue.get()
                self.queueLock.release()
                action()
            else:
                self.queueLock.release()
                time.sleep(1)
    
    def stop(self):
        self.exitFlag = True

    def start (self):
        try:
            self.active = True
            self.exitFlag = False
            for thread in self.threads:
                thread.start()
            
            
            while not self.mainQueue.empty():
                time.sleep(1)
            
            self.exitFlag = True

            for thread in self.threads:
                thread.join()
            
            self.active = False
        
        except (KeyboardInterrupt, SystemExit):
            print("Error")
            if self.exitFlag is False:
                print("Killing active Threads")
                self.stop()
        except Exception as e:
            print("Error {0}: {1}.".format(type(e), e))
            if self.exitFlag is False:
                print("Killing active Threads")
                self.stop()


class PoolThread (threading.Thread):

    def __init__(self, name, threadId, pool):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name
        self.pool = pool
        

    def run(self):
        print("Start " + self.name)
        self.pool.doAction()
        print("Finish " + self.name)


    