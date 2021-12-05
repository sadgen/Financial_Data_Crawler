# -*- coding: utf-8 -*-

import scrapy
from urllib import parse as urlparse
from scrapy_fd.items import ScrapyFdItem
import json
import pandas as pd
from sqlalchemy import create_engine

# 新浪网历史3秒级价格爬取
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
