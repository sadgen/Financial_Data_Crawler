# -*- coding: utf-8 -*-
import scrapy
from urllib import parse as urlparse
from scrapy_fd.items import ScrapyPriceDataItem
import json
import pandas as pd
from sqlalchemy import create_engine
from scrapy_fd.spiders.xpath_tables import parse_options_data
from lxml.html import parse
from urllib.request import urlopen
import re, ast

# 新浪网历史3秒级价格爬取
class SinaPriceDataSpider(scrapy.Spider):
    # spider的名字定义了Scrapy如何定位(并初始化)spider，所以其必须是唯一的
    name = "PriceData"

    # URL列表
    start_urls = ['http://vip.stock.finance.sina.com.cn/moneyflow/#!ssfx!']
    #  域名不在列表中的URL不会被爬取。
    allowed_domains = ['vip.stock.finance.sina.com.cn', 'market.finance.sina.com.cn']

    def start_requests(self):
        urls = [
            'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount?node=hs_a'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_total)

    def parse_total(self, response):
        stock_count = int(response.xpath('//p/text()').get().replace('"',''))
        inpagect = 100
        pagecount = int(stock_count/inpagect) + 1
        for i in range(1, pagecount+1):
            stock_urt = 'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=' + str(i) + '&num=' + str(inpagect) + '&sort=symbol&asc=1&node=hs_a'
            yield scrapy.Request(url=stock_urt, callback=self.parse_list)
            
    def parse_list(self, response, **kwargs):
        stocklist = json.loads(response.xpath('//p/text()').get())
        for s in stocklist:
            downdate_list = pd.bdate_range("2021-01-01", "2021-11-30", freq='C', weekmask=('1111100')).strftime("%Y-%m-%d").to_list()
            for downdate in downdate_list:
                # getpagenumb_idx = parse(urlopen('https://market.finance.sina.com.cn/transHis.php?symbol=' + s['symbol'] + '&date=' + downdate + '&page=1'))
                # try:
                #     a = re.findall('detailPages=\[.*\]',getpagenumb_idx.xpath('.//script//text()')[0])[0].replace('detailPages=','')
                #     pagenumb = int(ast.literal_eval(a.replace('detailPages=',''))[-1][0])
                # except:
                #     pagenumb = 100
                pagenumb = 1
                for i in range(1, pagenumb+1):
                    url = 'https://market.finance.sina.com.cn/transHis.php?symbol=' + s['symbol'] + '&date=' + downdate + '&page='+str(i)
                    yield scrapy.Request(url=url, callback=self.parsepricetable, cb_kwargs=dict(s=s['symbol'], d=downdate))

    def parsepricetable(self, response, s, d):
        pricetable = parse_options_data(response)
        if len(pricetable)==0:
            return []
        pricetable.insert(0,'code', s)
        pricetable.loc[:, '成交时间'] = pd.to_datetime(d + ' ' + pricetable.loc[:, '成交时间'])
        pricetable.loc[:, '成交价'] = pricetable.loc[:, '成交价'].astype('float')
        pricetable.loc[:, '价格变动'] = pricetable.loc[:, '价格变动'].replace('--',0).astype('float')
        pricetable.loc[:, '成交量(手)'] = pricetable.loc[:, '成交量(手)'].astype('float')
        pricetable.loc[:, '成交额(元)'] = pricetable.loc[:, '成交额(元)'].str.replace(',','').astype('float')
        pricetable.columns = ['code', 'tradetime', 'tradeprice', 'pricechange', 'volume', 'pricevolume', 'tradeattr']
        item = ScrapyPriceDataItem()
        item['pricedata'] = pricetable
        item['tag'] = "pricedata"
        return item