# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class TagItem(scrapy.Item):
    Tag = scrapy.Field()
    Sales = scrapy.Field()
    Site = scrapy.Field()
    RankChart = scrapy.Field()

class PageItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    ID = scrapy.Field()
    Title = scrapy.Field()
    Item_Num = scrapy.Field()
    Current_page = scrapy.Field()
    Page_avail = scrapy.Field()