# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SingleWork(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    ID = scrapy.Field()
    release_dtl = scrapy.Field()
    main_genre = scrapy.Field()
    title = scrapy.Field()
    circle = scrapy.Field()
    price_data = scrapy.Field()
    main_tags = scrapy.Field()
    historyData = scrapy.Field()
    extra_info = scrapy.Field()

class historyData(scrapy.Item):
    pass