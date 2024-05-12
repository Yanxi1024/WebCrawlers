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

class Distributer(threading.Thread):

    def __init__(self):
        super(Distributer, self).__init__()
        #buffer pool
        self.task_lis = list()

        #Mongodb
        self.conn = pymongo.MongoClient(uri)
        self.db = self.conn[srcdb]
        self.coll = self.db['secList']

        self.daemon = True

    def loadTasks(self):
        query = [
            {'$match': {'statu': 0, 'failed_count':{'$lte':10}}},
            {'$limit': 100}
        ]
        tasks = list(self.coll.aggregate(query))

        #load task into buffer pool, and set value of status 1(which means task has been load into line)
        for row in tasks:
            self.task_lis.append(row)
            self.coll.update_one({'article_url' : row['article_url']}, {'$set' : {'statu' : 1}})

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
            logger.info(f"{count}request success>>url:{url}")
            response.encoding = 'utf-8'
            html = response.text
            root = et.HTML(html)

            title_root = root.xpath("./body/div[@class = 'Article-box']/div[@class = 'Imgtxt-title']/div[@class = 'Imgtxt-name']/text()")
            title = getValueFromPath(title_root).strip()
            pub_date_root = root.xpath("./body/div[@class = 'Article-box']/div[@class = 'Imgtxt-title']/div[@class = 'Imgtxt-subti']/text()")
            pub_date = getValueFromPath(pub_date_root)

            abstract_root = root.xpath("./body/div[@class = 'Article-box']/div[@class = 'Imgtxt-cent']")
            if abstract_root:
                abstract = et.tostring(abstract_root[0], encoding = 'utf-8').decode('utf-8')
            else:
                abstract = ''

            return {'title' : title, 'pub_date' : pub_date, 'abstract' : abstract, 'html' : html, 'article_url' : url}, 1

    def run(self):
        while True:
            task = GlobalTaskQueue.get()

            res, flag = self.collectOnePage(task['article_url'])
            if flag:
                GlobalUpdateQueue.put(res)
            else:
                GlobalFailedQueue.put(task['article_url'])

class UpdateInfoProcessor(threading.Thread):

    def __init__(self):
        super(UpdateInfoProcessor, self).__init__()

        self.conn = pymongo.MongoClient(uri)
        self.db = self.conn[srcdb]
        self.coll = self.db['article']

        self.daemon = True

    def run(self):
        while True:
            data = GlobalUpdateQueue.get()
            current_time = getTime()

            self.coll.update_one(
                {'article_url' : data['article_url']},
                {
                    '$setOnInsert' : {'title' : data['title'], 'pub_date' : data['pub_date'], 'abstract' : data['abstract'], 'article_url' : data['article_url'], 'html' : data['html'], 'create_time' : current_time}, 
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
        self.coll = self.db['secList']

        self.daemon = True

    def run(self):
        event.wait()
        while True:
            data = GlobalFailedQueue.get()

            self.coll.update_one(
                {'article_url' : data},
                {
                    '$inc' : {'failed_count' : 1},
                    '$set' : {'statu' : 0}
                }
            )

def main():
    distributer = Distributer()
    distributer.start()

    for i in range(GlobalCollectorNum):
        Collector(i + 1).start()
    
    UpdateInfoProcessor().start()
    FailedInfoProcessor().start()

    # A supervisor. If main process finish all the tasks, then stop the process
    flag = 0
    while True:
        if GlobalTaskQueue.empty() and GlobalUpdateQueue.empty() and GlobalFailedQueue.empty():
            flag  += 1
            if flag > 5:
                print("process end")
                break
            print(f"main thread has check success {flag} times")
        time.sleep(15)

def getValueFromPath(root):
    if root:
        return root[0].strip()
    else:
        return ''

def test(current_url):
    response = requests.request(method = 'GET', url = current_url, headers = headers)
    response.encoding = 'utf-8'
    root = et.HTML(response.text)

    title_root = root.xpath("./body/div[@class = 'Article-box']/div[@class = 'Imgtxt-title']/div[@class = 'Imgtxt-name']/text()")
    title = getValueFromPath(title_root).strip()
    pub_date_root = root.xpath("./body/div[@class = 'Article-box']/div[@class = 'Imgtxt-title']/div[@class = 'Imgtxt-subti']/text()")
    pub_date = getValueFromPath(pub_date_root)
    print(pub_date)

    abstract_root = root.xpath("./body/div[@class = 'Article-box']/div[@class = 'Imgtxt-cent']")
    if abstract_root:
        abstract = et.tostring(abstract_root[0], encoding = 'utf-8').decode('utf-8')
    else:
        abstract = ''
    print(abstract)
    



if __name__ == '__main__':
    main()

    # test('https://www.cacm.org.cn/2016/04/05/7201/')
