# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScrapyFdItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    flowdata = scrapy.Field()
    tag = scrapy.Field()

class ScrapyPriceDataItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pricedata = scrapy.Field()
    tag = scrapy.Field()
