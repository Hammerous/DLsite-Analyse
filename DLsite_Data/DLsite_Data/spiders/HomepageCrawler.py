from pathlib import Path
from ..items import HomepageItem

import re
import pandas as pd
import scrapy
from bs4 import BeautifulSoup

class HomepageCrawler(scrapy.Spider):
    name = "HomepageCrawler"
    main_gerne = ["comic","cg","game","sound","etc"]

    def start_requests(self):
        urls = [
            "https://dojindb.net/r/all",
            "https://dojindb.net/r/day",
            "https://dojindb.net/r/week",
            "https://dojindb.net/r/month",
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        item = HomepageItem()
        item['ID'] = []
        item['Rank'] = []
        item['Sales'] = []

        # 处理主页面的数据
        self.extract_data(response, item)

        # 构建新的URL并继续爬取
        for genre in self.main_gerne:
            next_page_url = response.url + f"/?g={genre}"
            return scrapy.Request(next_page_url, callback=self.parse_next_page, meta={'item': item})

    def extract_data(self, response, item):
        # 由于网站排行所有作品的排行和ID所处的位置被打乱，因此使用包含两者的class进行爬取
        # 首先得到所有的作品信息集合
        work_list = response.xpath('//*[@class="col-xs-5 col-sm-3 col-md-3"]').extract()
        for work in work_list:
            soup = BeautifulSoup(work, "html.parser")
            numbers = soup.find('a', class_='work_thumb')['href']
            item['ID'].append(re.search(r'/w/(\d+)', numbers).group(1))
            item['Rank'].append(soup.find('span', class_='visible-xs label label-default label-ranking-mb').find('b').text.strip())
            item['Sales'].append(soup.find('span', class_='visible-xs label label-default label-ranking-mb').text.strip().replace(",", "").replace("本", "").replace(" ", ""))

    def parse_next_page(self, response):
        item = response.meta['item']
        # 处理下一个页面的数据
        self.extract_data(response, item)

        # 如果这是最后一个页面，返回item
        if response.url.split('/')[-1] == "?g={0}".format(self.main_gerne[-1]):
            name = response.url.split('/')[-2].replace(" ","")
            data = {'ID': item['ID'], 'Rank': item['Rank'], 'Sales': item['Sales']}
            df = pd.DataFrame(data)
            df.to_csv(name + '.csv', index=False)
            yield item

