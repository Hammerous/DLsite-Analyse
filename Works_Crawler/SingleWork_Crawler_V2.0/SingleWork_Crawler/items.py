# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SingleWork(scrapy.Item):
    ID = scrapy.Field()
    release_dtl = scrapy.Field()
    main_genre = scrapy.Field()
    title = scrapy.Field()
    verified = scrapy.Field()
    controvertial = scrapy.Field()
    price_data = scrapy.Field()
    circle = scrapy.Field()
    main_tags = scrapy.Field()
    historyData = scrapy.Field()
    extra_info = scrapy.Field()