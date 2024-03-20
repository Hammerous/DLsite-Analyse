from pathlib import Path
from ..items import DlsiteDataItem
from bs4 import BeautifulSoup

import re
import pandas as pd
import scrapy


class DLsiteSpider(scrapy.Spider):
    name = "DLsite"

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
        
        item = DlsiteDataItem()

        # 由于网站排行所有作品的排行和ID所处的位置被打乱，因此使用包含两者的class进行爬取
        # 首先得到所有的作品信息集合
        work_list = response.xpath('//*[@class="col-xs-5 col-sm-3 col-md-3"]').extract()
        name = response.xpath('//span[@class="bar_h1"]/text()').extract()
        work_ID = []
        work_Rank = []
        work_Sales = []

        for i in range(0, len(work_list)):
            # 使用BeautifulSoup解析html
            soup = BeautifulSoup(work_list[i], "html.parser")
        
            # 提取信息
            numbers = soup.find('a', class_ = 'work_thumb')['href']
            work_ID.append(re.search(r'/w/(\d+)', numbers).group(1))
            work_Rank.append(soup.find('span', class_ = 'visible-xs label label-default label-ranking-mb').find('b').text.strip())
            work_Sales.append(soup.find('span', class_ = 'visible-xs label label-default label-ranking-mb').\
                              text.strip().replace(",","").replace("本","").replace(" ",""))
            
        data = {'ID':work_ID, 'Rank':work_Rank, 'Sales': work_Sales}
        df = pd.DataFrame(data)
        df.to_csv(name[0] + '.csv', index=False)

        item['ID'] = work_ID
        item['Rank'] = work_Rank
        item['Sales'] = work_Sales

        yield item

