# -*- coding: utf-8 -*-
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings
import os

tag_csvpath = f"tags.csv"

i = 0
def crawl(runner):
    global i,Crawler_Mission
    if(i<len(Crawler_Mission)):
        try:
            print('\nCrawl Mission {0}/{1}: {2}'.format(i+1,len(Crawler_Mission), Crawler_Mission[i][0]))
            d = runner.crawl(*Crawler_Mission[i])
            #d.addBoth(sleep)
            d.addBoth(lambda _: crawl(runner))
            i+=1
        except Exception as e:
            print(e)
    else:
        os._exit(0)
    return d

def loop_crawl():
    runner = CrawlerRunner(get_project_settings())
    crawl(runner)
    reactor.run()

if __name__ == '__main__':
    i = 0
    Crawler_Mission = [['HomepageCrawler'],['TagsCrawler', tag_csvpath],\
                       ['PagesCrawler', tag_csvpath]]
    dirpath=os.path.dirname(os.path.abspath(__file__))
    # 获取当前路径
    os.chdir(dirpath)         # 切换到当前目录
    loop_crawl()