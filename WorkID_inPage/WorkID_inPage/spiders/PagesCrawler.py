import scrapy
from ..items import TagItem
import pandas as pd

class PagescrawlerSpider(scrapy.Spider):
    name = "PagesCrawler"
    allowed_domains = ["dojindb.net"]
    start_urls = ["http://dojindb.net/"]

    def start_requests(self):
        url = "https://dojindb.net/d/?c=site_tags"
        yield scrapy.Request(url=url, callback=self.parse,dont_filter=True)


    def parse(self, response):
        # Extract data from the response
        item = TagItem()
        item['Tag'] = []
        item['Sales'] = []
        item['Site'] = []
        item['RankChart'] = []

        name = response.xpath("//div[@class='data_h2']/b/text()").get().replace(" ","")
        data_box = response.xpath("//div[@class='data_box']")
        ranking_types = [each.get( )for each in data_box.xpath(".//div[@class='data_h3 mb15']/b/text()")]
        ranking_charts = data_box.xpath(".//div[@class='row']")
        
        for idx in range(len(ranking_charts)):
            ranking_lists = ranking_charts[idx].xpath("//table[@class='table table-bordered mb45']")
            for list in ranking_lists:
                list = list.xpath(".//tr")
                site = list.pop(0).xpath(".//text()").get()
                for idx_lst in range(1,len(list)):
                    row = list[idx_lst]
                    tag = row.xpath(".//td[1]//a/text()").get()
                    sales = row.xpath("td[2]/text()").get().replace(",","")
                    # Store the extracted data
                    item["Tag"].append(tag)
                    item["Sales"].append(sales)
                    item["Site"].append(site)
                    item["RankChart"].append(ranking_types[idx])
        data = {'Tag': item['Tag'], 'Sales': item['Sales'], 'Site': item['Site'], 'RankChart': item['RankChart']}
        df = pd.DataFrame(data)
        df.to_csv(name + '.csv', index=False)
        yield item