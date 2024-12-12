import requests
import pymongo
from lxml import etree as et
import threading
from loguru import logger
import re
from queue import Queue
import time

#related config
GlobalCollectorNum = 30
srcdb = 'cacm'
uri = 'mongodb://This is Mongodb URL'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"}

#buffer Queue
GlobalTaskQueue = Queue(GlobalCollectorNum * 2)
GlobalUpdateQueue = Queue(GlobalCollectorNum * 2)
GlobalFailedQueue = Queue()

#synchronization semaphore, ensure resetting status after loading all tasks
event = threading.Event()

count = 0

def getValueFromPath(root : list):
    if root:
        return root[0].strip()
    else:
        return ''

#get lcoal time and create module time fomat(8 pos)
def getTime():
    time_local = time.localtime(time.time())
    year = str(time_local.tm_year)
    mon = str(time_local.tm_mon)
    day = str(time_local.tm_mday)
    hour = str(time_local.tm_hour)
    mi = str(time_local.tm_min)
    sec = str(time_local.tm_sec)
    if len(mon) == 1:
        mon = '0' + mon
    if len(day) == 1:
        day = '0' + day
    if len(hour) == 1:
        hour = '0' + hour
    if len(mi) == 1:
        mi = '0' + mi
    if len(sec) == 1:
        sec = '0' + sec
    return f"{year}-{mon}-{day} {hour}:{mi}:{sec}"

def getValueFromPath(root):
    if root:
        return root[0]
    else:
        return ''

class Distributer(threading.Thread):

    def __init__(self):
        super(Distributer, self).__init__()
        #buffer pool
        self.task_lis = list()

        #Mongodb
        self.conn = pymongo.MongoClient(uri)
        self.db = self.conn[srcdb]
        self.coll = self.db['firList']

        self.daemon = True

    def loadTasks(self):
        query = [
            {'$match': {'statu': 0, 'failed_count':{'$lte':10}}},
            {'$limit': 100}
        ]
        tasks = list(self.coll.aggregate(query))

        #load task into buffer pool, and set value of status 1(which means task has been load into line)
        for row in tasks:
            self.task_lis.append(row['url'])
            self.coll.update_one({'url' : row['url']}, {'$set' : {'statu' : 1}})

    def run(self):
        while True:
            if not self.task_lis:
                self.loadTasks()
            if not self.task_lis:
                logger.debug("all task has been load")
                event.set()
                break

            # print(self.task_lis.pop())

            GlobalTaskQueue.put(self.task_lis.pop())

class Collector(threading.Thread):

    def __init__(self, t_id):
        super(Collector, self).__init__()

        self.t_id = t_id

        self.daemon = True

    def collectOnePage(self, url):

        global count
        count += 1

        try:
            response = requests.request(method = 'GET', url = url, headers = headers, timeout = 30)
        except:
            logger.warning(f"{count}>>request failed>>url:{url}")
            return url, 0
        else:
            logger.debug(f"{count}request success>>url:{url}")
            response.encoding = 'utf-8'
            root = et.HTML(response.text)

            res = []
            for li in root.xpath("./body/div[@class = 'Listpage-box']/div[@class = 'Listpage-cent']/ul/li[@class = 'sc-clearfix']"):
                article_url = getValueFromPath(li.xpath("./a[@href]/@href"))
                title = getValueFromPath(li.xpath("./a[@href]/text()"))
                logger.info(f"title:{title}")
                res.append({"title" : title, 'article_url' : article_url})

            return res, 1

    def run(self):
        while True:
            task = GlobalTaskQueue.get()

            res, flag = self.collectOnePage(task)
            if flag:
                for row in res:
                    GlobalUpdateQueue.put(row)
            else:
                GlobalFailedQueue.put(res)

class UpdateInfoProcessor(threading.Thread):

    def __init__(self):
        super(UpdateInfoProcessor, self).__init__()

        self.uri = 'mongodb://rwany:ztdata@mongo3:27017,mongo4:27017,mongo5:27017/eagejournal?authSource=admin&replicaSet=rs0&w=majority&wtimeoutMS=20000&readConcernLevel=majority&readPreference=secondaryPreferred&connectTimeoutMS=20000&socketTimeoutMS=200000'
        self.conn = pymongo.MongoClient(self.uri)
        self.db = self.conn[srcdb]
        self.coll = self.db['secList']

        self.daemon = True

    def run(self):
        while True:
            data = GlobalUpdateQueue.get()
            current_time = getTime()

            self.coll.update_one(
                {'article_url' : data['article_url']},
                {
                    '$setOnInsert' : {'title' : data['title'], 'article_url' : data['article_url'], 'statu' : 0, 'failed_count' : 0, 'create_time' : current_time}, 
                    '$set' : {'update_time' : current_time}
                },
                upsert = True
            )

class FailedInfoProcessor(threading.Thread):

    def __init__(self):
        super(FailedInfoProcessor, self).__init__()

        self.uri = 'mongodb://rwany:ztdata@mongo3:27017,mongo4:27017,mongo5:27017/eagejournal?authSource=admin&replicaSet=rs0&w=majority&wtimeoutMS=20000&readConcernLevel=majority&readPreference=secondaryPreferred&connectTimeoutMS=20000&socketTimeoutMS=200000'
        self.conn = pymongo.MongoClient(self.uri)
        self.db = self.conn[srcdb]
        self.coll = self.db['firList']

        self.daemon = True

    def run(self):
        event.wait()
        while True:
            data = GlobalFailedQueue.get()

            self.coll.update_one(
                {'url' : data},
                {
                    '$inc' : {'failed_count' : 1},
                    '$set' : {'statu' : 0}
                }
            )

def test():
    response = requests.request(method = 'GET', headers = headers, url = 'https://www.cacm.org.cn/category/hyyw/page/1/', timeout = 30)
    response.encoding = 'utf-8'
    root = et.HTML(response.text)

    res = []
    for li in root.xpath("./body/div[@class = 'Listpage-box']/div[@class = 'Listpage-cent']/ul/li[@class = 'sc-clearfix']"):
        article_url = getValueFromPath(li.xpath("./a[@href]/@href"))
        title = getValueFromPath(li.xpath("./a[@href]/text()"))
        res.append({"title" : title, 'article_url' : article_url})
        



def main():
    distributer = Distributer()
    distributer.start()

    for i in range(GlobalCollectorNum):
        Collector(i + 1).start()
    
    UpdateInfoProcessor().start()
    FailedInfoProcessor().start()

    # A supervisor. If main process finish all the tasks, then stop the the process
    flag = 0
    while True:
        if GlobalTaskQueue.empty() and GlobalUpdateQueue.empty() and GlobalFailedQueue.empty():
            flag  += 1
            if flag > 5:
                print("process end")
                break
            print(f"main thread has check success {flag} times")
        time.sleep(30)

if __name__ == '__main__':
    main()
    # test()

    # print(re.sub("\.\./", '', '../../info/1133/80753.htm'))
