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
srcdb = 'This us srcdb'
uri = 'mongodb://This is MongoDB'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"}
proxies = {
    'http' : '192.168.**.***:****',
    'https' : '192.168.**.***:****'
}

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

            title = getValueFromPath(root.xpath("./body//h1[@class = 'title-toc']/text()")).strip()
            sub_root = root.xpath("./body/div[@id = 'wrapper']/div[@class = ' ']/div[@class = 't-stacked ']/div[@class = 't-stacked row main-content-grid document-toc']/div[@id = 'content']/div[@class = 't-html title-abstract']")
            if len(sub_root) > 1:
                abstract = getValueFromPath(sub_root[1].xpath("./p")[1].xpath("./text()")).strip()
                if len(sub_root[1].xpath("./p")) > 2:
                    reg = re.compile(".*</em>|</p>|</p>", re.S)
                    host_organ = re.sub(reg, '', et.tostring(sub_root[1].xpath("./p")[2], encoding = 'utf-8').decode('utf-8')).strip()
                else:
                    host_organ = ''
            else:
                abstract = ''
                host_organ = ''
            authors_root = root.xpath("//span[@class = 't-listitem t-listitem-authors']/text()")
            author = ';'.join(e.strip() for e in authors_root)

            pub_date_1 = str(getValueFromPath(root.xpath("//span[@class = 't-toc-prop-value t-toc-prop-value-pubdate']/text()"))).strip()
            pub_date_2 = str(getValueFromPath(root.xpath("./head/meta[@name = 'citation_publication_date']/@content"))).strip()
            if not pub_date_1 == '':
                pub_date = pub_date_1
            elif not pub_date_2 == '':
                pub_date = pub_date_2
            else:
                pub_date = ''

            return {'title' : title, 'article_url' : url, 'author' : author, 'abstract' : abstract, 'html' : html, 'host_organ' : host_organ, 'pub_date' : pub_date}, 1

    def run(self):
        while True:
            task = GlobalTaskQueue.get()

            res, flag = self.collectOnePage(task['article_url'])
            if flag:
                if res['title'] == '':
                    res['title'] = task['title']
                GlobalUpdateQueue.put(res)
            else:
                GlobalFailedQueue.put(res)

class UpdateInfoProcessor(threading.Thread):

    def __init__(self):
        super(UpdateInfoProcessor, self).__init__()

        self.uri = uri
        self.conn = pymongo.MongoClient(self.uri)
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
                    '$setOnInsert' : {'title' : data['title'], 'article_url' : data['article_url'], 'author' : data['author'], 'pub_date' : data['pub_date'], 'abstract' : data['abstract'], 'host_organ' : data['host_organ'], 'html' : data['html'], 'create_time' : current_time}, 
                    '$set' : {'update_time' : current_time}
                },
                upsert = True
            )

class FailedInfoProcessor(threading.Thread):

    def __init__(self):
        super(FailedInfoProcessor, self).__init__()

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

    # A supervisor. If main process finish all the tasks, then stop the the process
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
        return root[0]
    else:
        return ''

def test(current_url):
    response = requests.request(method = 'GET', url = current_url, proxies = proxies, headers = headers)
    response.encoding = 'utf-8'
    root = et.HTML(response.text)

    title = getValueFromPath(root.xpath("./body//h1[@class = 'title-toc']/text()")).strip()
    sub_root = root.xpath("./body/div[@id = 'wrapper']/div[@class = ' ']/div[@class = 't-stacked ']/div[@class = 't-stacked row main-content-grid document-toc']/div[@id = 'content']/div[@class = 't-html title-abstract']")
    abstract = getValueFromPath(sub_root[1].xpath("./p")[1].xpath("./text()")).strip()
    reg = re.compile(".*</em>|</p>|</p>", re.S)
    host_organ = re.sub(reg, '', et.tostring(sub_root[1].xpath("./p")[2], encoding = 'utf-8').decode('utf-8')).strip()
    authors_root = root.xpath("//span[@class = 't-listitem t-listitem-authors']/text()")
    author = ';'.join(e.strip() for e in authors_root)
    
if __name__ == '__main__':
    main()

    # test('https://publications.arl.org/ARL-Annual-Salary-Survey-2011-2012/')

    # conn = pymongo.MongoClient(uri)
    # db = conn[srcdb]
    # coll = db['secList']
    # for row in coll.find():
    #     print('------------------------------------')
    #     print(row['article_url'])
    #     if test(row['article_url']) == "":
    #         break

        # re.sub("", '', '')
