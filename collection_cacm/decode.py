from myLib import dc
import sqlite3
from lxml import etree as et
from pymongo import MongoClient
from loguru import logger
import re
import time

srcdb = 'cacm'

db_name = 'zt_pre_meta_a'
sub_db_id = 'This is sub_db_id'
doc_type = 'This is doc_type'
uri_final = 'mongodb://This is MongoDB'
coll_name_final = f"pre_a_{sub_db_id}_{doc_type}_{srcdb}"

def getRawid(url):
    return re.sub('https://www.cacm.org.cn', '', url).strip().strip('/')

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

def decode():
    uri = uri_final
    Mongo_conn = MongoClient(uri)
    Mongo_db = Mongo_conn[srcdb]
    coll = Mongo_db['article']

    article_list = []
    for row in coll.find({}, {'_id' : 0, 'html' : 0}):
        article_list.append(row)

    local_db_name = dc.createDb(srcdb, '2022_zt_template_zx.db3')
    local_db_conn = sqlite3.connect(local_db_name)
    local_db_cursor = local_db_conn.cursor()

    Mongo_conn = MongoClient(uri_final)
    Mongo_db = Mongo_conn[db_name]
    coll = Mongo_db[coll_name_final]

    batch = dc.getBatch(srcdb)
    process_type = '1'
    schema_version = '2022.8.1'
    language = 'ZH'
    country = 'CN'
    medium = '2'
    doc_type = '14'

    count = 0
    current_time = getTime()
    for row in article_list:
        count += 1
        rawid = getRawid(row['article_url'])
        down_date = re.sub('-| .*', '', row['update_time'])
        baseid = dc.GetBaseid('00826', rawid)
        title = dc.transferDoubleQuote(row['title'])
        abstract = dc.transferDoubleQuote(row['abstract'])
        srcdb_url = f"{srcdb}@{row['article_url']}"
        if row['pub_date'].strip() == '':
            pub_date = '00000000'
        else:
            year = re.sub('年.*', '', re.sub("发布日期：", '', row['pub_date']).strip()).strip()
            if len(year) == 2:
                year = f"19{year}"
            month = re.sub('月.*', '', re.sub(".*年", '', row['pub_date'])).strip()
            for i in range(2 - len(month)):
                month = f"0{month}"
            day = re.sub('日.*', '' , re.sub('.*月', '', row['pub_date'])).strip()
            for i in range(2 - len(day)):
                day = f"0{day}"
            pub_date = f"{year}{month}{day}"
        pub_year = pub_date[0 : 4]
        logger.info(f"{count}>>title:{title}")

        local_db_cursor.execute('INSERT INTO base_doc (baseid, srcdb, batch, schema_version, title, abstract, srcdb_url, pub_date, pub_year, language, country, rawid, process_type, medium, down_date, doc_type'
                           + ') VALUES (\"'
                           + baseid + '\",\"' + srcdb + '\",\"' + batch + '\",\"' + schema_version + '\",\"' + title + '\",\"' + abstract + '\",\"' + srcdb_url + '\",\"' + pub_date + '\",\"' + pub_year + '\",\"' + language + '\",\"' + country + '\",\"' + rawid + '\",\"' + process_type + '\",\"' + medium + '\",\"' + down_date + '\",\"' + doc_type
                           + "\")")
        
        coll.update_one(
                {'rawid' : rawid},
                {
                    '$setOnInsert' : {'create_time' : current_time},
                    '$set' : {'baseid' : baseid, 'srcdb' : srcdb, 'batch' : batch, 'schema_version' : schema_version, 'title' : title, 'abstract' : abstract, 'srcdb_url' : srcdb_url, 'pub_date' : pub_date, 'pub_year' : pub_year, 'language' : language, 'country' : country, 'rawid' : rawid, 'process_type' : process_type, 'medium' : medium, 'down_date' : down_date, 'doc_type' : doc_type, 'update_time' : current_time}
                },
                upsert = True
            )
    local_db_conn.commit()
    local_db_cursor.close()
    local_db_conn.close()

def main():
    decode()

if __name__ == '__main__':
    main()