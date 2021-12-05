# -*- coding: utf-8 -*-

from datetime import datetime
from lxml.html import parse
from urllib.request import urlopen
import pandas as pd
# 查找文档中所有的table,　返回一个列表
import re

def _unpack(row, kinds):
    xpath_str = ''
    for kind in kinds:
        xpath_str = xpath_str + './/{0}/text()|'.format(kind)
    xpath_str = xpath_str[:-1]
    elts = row.xpath(xpath_str).getall()
    return elts

def parse_options_data(response):
    tables = response.xpath('//table')
    if len(tables)==0:
        return[]
    rows = tables[0].xpath('.//tr')
    header = _unpack(rows[0], kinds=['th','td'])
    data = [_unpack(r, kinds=['th','td','h6','h5','h1']) for r in rows[1:]]
    result = pd.DataFrame(data, columns=header)
    return result

if __name__=='__main__':
    # 使用的是Python3, Python2可能需要from urllib2 import urlopen
    doc = parse(urlopen('https://market.finance.sina.com.cn/transHis.php?symbol=sz000002&date=2021-11-29&page=78'))
    # 打开url, 并且使用parse方法转化为可以使用xpath查找的格式
    tables = doc.xpath('//table')
    result = parse_options_data(tables[0])
    result.loc[:, '成交时间'] = pd.to_datetime('2021-10-20 ' + result.loc[:, '成交时间'])
    result.loc[:, '成交价'] = result.loc[:, '成交价'].astype('float')
    result.loc[:, '价格变动'] = result.loc[:, '价格变动'].replace('--',0).astype('float')
    result.loc[:, '成交量(手)'] = result.loc[:, '成交量(手)'].astype('float')
    result.loc[:, '成交额(元)'] = result.loc[:, '成交额(元)'].str.replace(',','').astype('float')