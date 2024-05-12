import os
import time
import hashlib
import base64
from stdnum import isbn
import re
# import pandas as pd

def createDb(source_name, db_template):
    f"""根据数据库模板创建数据库，根据scrdb和创建时间为数据库命名并返回数据库名"""
    time_local = time.localtime(time.time())
    year = str(time_local.tm_year)
    mon = str(time_local.tm_mon)
    day = str(time_local.tm_mday)
    if len(mon) == 1:
        mon = '0' + mon
    if len(day) == 1:
        day = '0' + day
    db_name =  source_name + '_' + year + mon + day + '.db3'
    os.system(f"copy \".\\db_template\\{db_template}\" \".\\" + db_name + "\"")
    return db_name

def getValidIsbn(data : str):
    F"""删除信息中的短横，并判断isbn和eisbn的有效性"""
    if not data == '':
        data = re.sub('-', '', data)
        if not isbn.is_valid(data):
            data = ''
    return data

def BaseEncodeID(strRaw):
    r"""自定义base编码"""
    strEncode = base64.b32encode(strRaw.encode('utf8')).decode('utf8')

    if strEncode.endswith('======'):
        strEncode = '%s%s' % (strEncode[0:-6], '0')
    elif strEncode.endswith('===='):
        strEncode = '%s%s' % (strEncode[0:-4], '1')
    elif strEncode.endswith('==='):
        strEncode = '%s%s' % (strEncode[0:-3], '8')
    elif strEncode.endswith('='):
        strEncode = '%s%s' % (strEncode[0:-1], '9')

    table = str.maketrans('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210')
    strEncode = strEncode.translate(table)

    return strEncode

def GetBaseid(sub_db_id, rawid, case_insensitive=False):
    r"""
    由 sub_db_id 和 rawid 得到 lngid。
    case_insensitive 标识源网站的 rawid 是否区分大小写
    """
    uppercase_rawid = ''  # 大写版 rawid
    if case_insensitive:  # 源网站的 rawid 区分大小写
        for ch in rawid:
            if ch.upper() == ch:
                uppercase_rawid += ch
            else:
                uppercase_rawid += ch.upper() + '_'
    else:
        uppercase_rawid = rawid.upper()

    limited_id = uppercase_rawid  # 限长ID
    if len(uppercase_rawid.encode('utf8')) > 20:
        limited_id = hashlib.md5(uppercase_rawid.encode('utf8')).hexdigest().upper()
    else:
        limited_id = BaseEncodeID(uppercase_rawid)        

    baseid = sub_db_id + limited_id

    return baseid

def getBatch(srcdb):
    f"""根据srcdb和当前日期拼接得到patch"""
    time_local = time.localtime(time.time())
    year = str(time_local.tm_year)
    mon = str(time_local.tm_mon)
    day = str(time_local.tm_mday)
    if len(mon) == 1:
        mon = '0' + mon
    if len(day) == 1:
        day = '0' + day
    return f'{srcdb}@' + year + mon + day + '00'

def setLanguage(raw_language : str):
    f"""将表中表示语言的信息转为标准的语言表示信息"""
    raw_language = raw_language.strip()
    if raw_language == 'ENG':
        return 'EN'
    elif raw_language == 'GER':
        return 'DE'
    elif raw_language == 'FRE':
        return 'FR'
    elif raw_language == 'SPA':
        return 'ES'
    elif raw_language == 'POR':
        return 'PT'
    elif raw_language == 'ITA':
        return 'IT'
    elif raw_language == 'DUT':
        return 'NL'
    elif raw_language == 'LAT':
        return 'LA'
    elif raw_language == 'GRE':
        return 'EL'
    elif raw_language == 'DAN':
        return 'DA'
    elif raw_language == 'CRO':
        return 'HT'
    elif raw_language == 'CHI':
        return 'ZH'
    elif raw_language == 'CH':
        return 'ZH'
    elif raw_language == 'JPN':
        return 'JA'
    elif raw_language == 'KOR':
        return 'KO'
    elif raw_language == 'TIB':
        return 'BO'
    elif raw_language == 'RUS':
        return 'RU'
    elif raw_language == 'MUL':
        return 'ZH'
    elif raw_language == 'ARA':
        return 'AR'
    elif raw_language == '0':
        return 'ZH'
    else:
        # print(raw_language)
        #搜索列表外的语言时使用(改正了中文)
        return 'ZH'
    
def transferDoubleQuote(data):
    f"""对数据中的双引号进行转义"""
    if not data == '':
        data = re.sub('\"', '\"\"', data)
    return data

def setNum(data : str):
    f"""消除浮点数"""
    return re.sub('\.0*', '', data)

def transferBlock(data : str):#改
    data = re.sub('，', ';', data)
    data = re.sub('；', ';', data)
    data = re.sub(',', ';', data)
    data = re.sub('  ', ';', data)
    return data

def removeBlank(data : str):
    f"""消除分号间隔间的空格"""
    res = []
    for e in data.split(';'):
        res.append(e.strip())
    return ';'.join(res)

def subBlock(data : str):
    f"""将中文分号 中文逗号 英文逗号 双空格转为英文分号"""
    data = re.sub('；', ';', data)
    data = re.sub(',', ';', data)
    data = re.sub('，', ';', data)
    data = re.sub('\s{2,}', ';', data)
    return data

def setPubFormat(pub_year, pub_date):
    f"""
    传入的出版年和出版日期应当为纯数字的字符串
    返回格式标准的出版年和出版日期
    """
    if pub_year == '':
        if pub_date == '':
            return '1000', '10000000'
        else:
            return pub_date[0: 4], pub_date
    else:
        if pub_date == '':
            return pub_year, pub_year + '1000'
        else:
            return pub_year, pub_date
        
# def setCountry(country : str):
#     country = country.strip()
#     if country == 'CN'
        
def setAuthor(author):
    author = transferBlock(author)
    author = removeBlank(author)
    author = re.sub('主编$', '', author)
    author = re.sub('编著$', '', author)
    author = re.sub('编译$', '', author)
    author = re.sub('导读$', '', author)
    return author.strip()
        
def test():
    print('This is dc.py')
