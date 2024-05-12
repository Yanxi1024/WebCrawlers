import requests
import pymongo
from lxml import etree as et
# import threading
from loguru import logger
import re
# from queue import Queue
import time
import json

#related config
srcdb = 'cacmhyywinfo'
url = f'https://www.cacm.org.cn/category/hyyw/'
uri = 'mongodb://This is Mongodb URL'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"}

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

def getPageNumFromPath(root : list):
    if root:
        return int(re.sub('[^\d]', '', root[0]))
    return -1

def getPageNum(url):
        try:
            response = requests.request(method = 'GET', url = url, headers = headers, timeout = 30)
        except:
            logger.error(f"request main page failed! Process cannot continue!>> main page url:{url}")
            return -1
        else:
            response.encoding = 'utf-8'
            root = et.HTML(response.text)
            page_num_root = root.xpath("./body/div[@class = 'Listpage-box']/div[@id = 'paging']/div[@class = 'pagination']/ul/li")
            # print(len(page_num_root))
            if len(page_num_root) < 4:
                logger.error(f"get page num failed! Process cannot continue!>> main page url:{url}")
                return -1
            return getPageNumFromPath(page_num_root[-1].xpath("./span/text()"))
        
def main():
    page_num = getPageNum(url)

    res = []
    if page_num == -1:
        return -1
    logger.debug(f"page_num:{page_num}")
    for i in range(1, page_num + 1):
        sub_page_url = f'https://www.cacm.org.cn/category/hyyw/page/{str(i)}/'
        logger.info(f"{page_num}/{i}>>url:{sub_page_url}")
        res.append(sub_page_url)

    conn = pymongo.MongoClient(uri)
    db = conn[srcdb]
    coll = db['firList']
    current_time = getTime()
    for row in res:
        coll.update_one(
                    {'url' : row},
                    {
                        '$setOnInsert' : {'url' : row, 'create_time' : current_time}, 
                        '$set' : {'update_time' : current_time, 'failed_count' : 0, 'statu' : 0}
                    },
                    upsert = True
                )
    conn.close()

if __name__ == '__main__':
    main()
    # print(getPageNum(url))