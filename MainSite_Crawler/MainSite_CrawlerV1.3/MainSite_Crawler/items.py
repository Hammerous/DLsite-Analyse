# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class mainItem(scrapy.Item):
    Site = scrapy.Field()           # str type
    ID = scrapy.Field()             # str type
    Title = scrapy.Field()             # str type
    Circle = scrapy.Field()         # str type
    Sales = scrapy.Field()          # int type
    Favorites = scrapy.Field()      # int type
    Price = scrapy.Field()          # float type
    Rating = scrapy.Field()         # float type
    Reviews = scrapy.Field()        # int type
    CmtReviews = scrapy.Field()     # int type
    Tags = scrapy.Field()           # str type
    Genre = scrapy.Field()          # str type
    Series = scrapy.Field()         # str type
    Ranking_info = scrapy.Field()   # dict type
    Rating_info = scrapy.Field()    # dict type

class FANZACrawlerItem(mainItem):
    Sub_Genre = scrapy.Field()      # str type
    WorkType = scrapy.Field()       # str type

class DLsiteCrawlerItem(mainItem):
    Creator = scrapy.Field()        # str type
    Senario = scrapy.Field()        # str type
    Illust = scrapy.Field()         # str type
    CV = scrapy.Field()             # str type
    Music = scrapy.Field()          # str type
    Age_Restrict = scrapy.Field()   # str type
    Other_info = scrapy.Field()   # str type
    language = scrapy.Field()   # str type
    Reviews_info = scrapy.Field()   # dict type
    Translation_info = scrapy.Field()   # dict type