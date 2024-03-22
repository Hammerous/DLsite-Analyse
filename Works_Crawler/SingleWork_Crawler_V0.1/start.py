#'''
from scrapy import cmdline
import os
dirpath=os.path.dirname(os.path.abspath(__file__))
# 获取当前路径
os.chdir(dirpath)         # 切换到当前目录
if os.path.exists('all.log'):
    os.remove('all.log')
ID = 440044
cmdline.execute("scrapy crawl WorkCrawler -a workID={} -s LOG_FILE=all.log".format(ID).split())
#'''
'''
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process
from scrapy.utils.project import get_project_settings
from bilibili_spider.spiders.spider import SpiderSpider

pages = 2

def start(page):
    print(f"page={page}")
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    spider = SpiderSpider()
    process.crawl(spider, page=page)
    process.start()

if __name__ == "__main__":
    for i in range(1,pages+1):
        page = i
        p = Process(target=start, args=(page,))
        p.start()
'''
# 项目不同， 改为你项目的名字