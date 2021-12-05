# -*- coding: utf-8 -*-

import scrapy
from urllib import parse as urlparse
from scrapy_fd.items import ScrapyFdItem
import json
import pandas as pd
from sqlalchemy import create_engine

# 新浪网资金流向，调整页数避免下载过多
class SinaSpider(scrapy.Spider):
    # spider的名字定义了Scrapy如何定位(并初始化)spider，所以其必须是唯一的
    name = "sina_fd"

    # URL列表
    start_urls = ['http://vip.stock.finance.sina.com.cn/moneyflow/#!ssfx!']
    #  域名不在列表中的URL不会被爬取。
    allowed_domains = ['vip.stock.finance.sina.com.cn']

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
            url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssc_qsfx_lscjfb?daima=' + s['symbol']
            yield scrapy.Request(url=url, callback=self.parse_flownum, cb_kwargs=dict(s=s['symbol']))

    def parse_flownum(self, response, s):
        flownum = int(response.xpath('//p/text()').get().replace('"',''))
        inpagect = 1000
        pagecount = int(flownum/inpagect) + 1
        for i in range(1, pagecount+1):
            url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=' + str(i) + '&num=' + str(inpagect) + '&sort=opendate&asc=0&daima=' + s
            yield scrapy.Request(url=url, callback=self.parse_flowdata, cb_kwargs=dict(s=s))
    
    def parse_flowdata(self, response, s):
        flowdata = pd.DataFrame(json.loads(response.xpath('//p/text()').get()))
        flowdata.insert(0,'code', s)
        item = ScrapyFdItem()
        item['flowdata'] = flowdata
        yield item