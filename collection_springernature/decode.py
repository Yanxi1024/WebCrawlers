from myLib import dc
import sqlite3
import requests
from lxml import etree as et
from pymongo import MongoClient
from loguru import logger
import time
import re
# import pandas as pd

srcdb = 'springernature'

db_name = 'zt_pre_meta_a'
sub_db_id = '00***'
doc_type = '**'
uri_final = 'mongodb://This is MongoDB'

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

def getRawid(url : str):
    url = re.sub('https://publications.arl.org', '', url).strip()
    return url.strip('/')

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
    language = 'EN'
    country = 'US'
    medium = '2'

    count = 0
    current_time = getTime()

    for row in article_list:
        count += 1
        rawid = getRawid(row['article_url'])
        baseid = dc.GetBaseid('00845', rawid)
        title = dc.transferDoubleQuote(row['title'])
        author = dc.transferDoubleQuote(row['author'])
        abstract = dc.transferDoubleQuote(row['abstract'])
        # host_organ = dc.transferDoubleQuote(row['host_organ'])
        srcdb_url = f"{srcdb}@{row['article_url'].strip()}"
        down_date = re.sub('[^\d]', '', re.sub(' .*', '', row['update_time'].strip()))
        if row['pub_date'].strip() == '':
            pub_date = '10000000'
        else:
            pub_date = re.sub("[^\d]", '', row['pub_date'])
        pub_year = pub_date[0 : 4]
        logger.info(f"{count}>>title:{title}")

        local_db_cursor.execute('INSERT INTO base_doc (baseid, srcdb, batch, schema_version, title, abstract, srcdb_url, pub_date, pub_year, language, country, rawid, process_type, down_date, medium, author, pub_year, pub_date'
                           + ') VALUES (\"'
                           + baseid + '\",\"' + srcdb + '\",\"' + batch + '\",\"' + schema_version + '\",\"' + title + '\",\"' + abstract + '\",\"' + srcdb_url + '\",\"' + pub_date + '\",\"' + pub_year + '\",\"' + language + '\",\"' + country + '\",\"' + rawid + '\",\"' + process_type + '\",\"' + down_date + '\",\"' + medium + '\",\"' + author + '\",\"' + '1000' + '\",\"' + '10000000'
                           + "\")")
        
        coll.update_one(
                {'rawid' : rawid},
                {
                    '$setOnInsert' : {'create_time' : current_time},
                    '$set' : {'baseid' : baseid, 'srcdb' : srcdb, 'batch' : batch, 'schema_version' : schema_version, 'title' : title, 'abstract' : abstract, 'srcdb_url' : srcdb_url, 'pub_date' : pub_date, 'pub_year' : pub_year, 'language' : language, 'country' : country, 'rawid' : rawid, 'process_type' : process_type, 'down_date' : down_date, 'medium' : medium, 'author' : author, 'pub_year' : '1000', 'pub_date' : '10000000', 'update_time' : current_time}
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



